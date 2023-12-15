import sys
from util import *
from pyspark import SparkContext
import time
from schema import Schema
import matplotlib.pyplot as plt

def initiateSpark():
	global sc
	sc = SparkContext("local[*]")
	sc.setLogLevel("ERROR")

def loadData(file_path:str):
	file = sc.textFile(file_path)
	return file

def loadEvents(file_path:str):
	events = loadData(file_path)
	events = events.map(lambda x : x.split(','))
	return events

def getCPUDistribution():
	print("What is the distribution of the machines according to their CPU capacity?")
	print("-------------------------------------")

	cpu_index = schema.getFieldNoByContent("machine_events", "CPU")
	
	# #Get the index of the machine ID in the task event
	machine_id_index_task_event = int(schema.getFieldNoByContent("machine_events", "machine ID")) 

	machine_cpu_distribution = machine_events.filter(lambda x: x[cpu_index]!='').map(lambda x: (x[machine_id_index_task_event], x[cpu_index])).groupByKey()
	res = machine_cpu_distribution.mapValues(lambda x: list(x)[0])
	cpu_distribution = res.map(lambda x: (x[1] , 1)).reduceByKey(lambda x,y:x+y).collect()
	plotDistribution(cpu_distribution , "CPU" , "Machines", "Distribution of CPU Across Machines")

	
	print("=========================================================================================\n")



	
def isCPUChanging():
	cpu_index = schema.getFieldNoByContent("machine_events", "CPU")
	
	# #Get the index of the machine ID in the task event
	machine_id_index_task_event = int(schema.getFieldNoByContent("machine_events", "machine ID")) 

	distribution = machine_events.filter(lambda x: x[cpu_index]!='').map(lambda x: (x[machine_id_index_task_event], x[cpu_index])).groupByKey()
	final = distribution.mapValues(lambda x: set(x)).filter(lambda x: len(x[1])>1).collect()
	print(final)

def computePowerLost():
	
	print("What is the percentage of computational power lost due to maintenance (a machine went offline and reconnected later)?")
	print("-------------------------------------")


	REMOVE = '1'
	event_type_index = int(schema.getFieldNoByContent("machine_events", "event type")) 

	fail_events = machine_events.filter(lambda x: x[event_type_index] == REMOVE)
	
	def computeDuration(timestamp):
		''' 
			Each record has a timestamp, which is in microseconds since 600 seconds before the
			beginning of the trace period, and recorded as a 64 bit integer (i.e., an event 20 seconds
			after the start of the trace would have a timestamp=620s).
		'''
		ref_offset_ms = 600_000_000
		return (int(timestamp) - ref_offset_ms)/1_000_000

	cpu_index = int(schema.getFieldNoByContent("machine_events", "CPU")) 
	timestamp_index = int(schema.getFieldNoByContent("machine_events", "time")) 

	#Remove the entries that don't have CPU
	filtered_events = machine_events.filter(lambda x: x[cpu_index] != '' )
	#Compute the lost power from the failed events
	lost_power = fail_events.map(lambda x: float(x[cpu_index])*computeDuration(x[timestamp_index])).reduce(lambda x,y: x+y)
	#Compute total power
	total_power = filtered_events.map(lambda x: float(x[cpu_index])*computeDuration(x[timestamp_index])).reduce(lambda x,y: x+y)
	percentage_lost = (lost_power/total_power) * 100
	
	#TODO: Probably not the solution. Maybe find the first occurance of a restart and use the difference as the time passed?
	print(f"Percentage of power lost: {round(percentage_lost,2)}%")
	print("=========================================================================================\n")



def getSchedClassDistribution():
	sched_class_index_task_events = schema.getFieldNoByContent("task_events" , "scheduling class")
	sched_class_index_job_events = schema.getFieldNoByContent("job_events" , "scheduling class")
	
	task_distribution = task_events.map(lambda x: (x[sched_class_index_task_events] , 1)).reduceByKey(lambda x,y:x+y).collect()
	job_distribution = job_events.map(lambda x: (x[sched_class_index_job_events] , 1)).reduceByKey(lambda x,y:x+y).collect()

	task_distribution.sort(key=lambda x: x[0])
	job_distribution.sort(key=lambda x: x[0])
	plotDistribution(task_distribution , "Class" , "Tasks", "Distribution of Tasks Across Scheduling Classes")
	plotDistribution(job_distribution , "Class" , "Jobs", "Distribution of Jobs Scheduling Classes")

	print(f"Task distribution {task_distribution}")
	print(f"Job distribution {job_distribution}")


def getClassEvictProbability():
	EVICT = '2'
	event_type_index_task_event = schema.getFieldNoByContent("task_events", "event type")
	sched_class_index_task_events = schema.getFieldNoByContent("task_events" , "scheduling class")

	task_sched_evitctions = task_events.filter(lambda x: x[event_type_index_task_event] == EVICT).map(lambda x: (int(x[sched_class_index_task_events]) , 1)).reduceByKey(lambda x,y:x+y).collect()
	task_sched_evitctions = (dict(task_sched_evitctions))
	total_tasks_per_class = task_events.map(lambda x: (int(x[event_type_index_task_event])  , 1)).reduceByKey(lambda x,y : x+y)

	eviction_probability = total_tasks_per_class.map(lambda x: (x[0] , round(task_sched_evitctions[x[0]]*100/x[1],2) ) if x[0] in task_sched_evitctions else (x[0] , 0)).collect()
	print(eviction_probability)
	#TODO 



def getTasksRunningOnMachines():
	job_id_index_task_events = schema.getFieldNoByContent("task_events" , "job ID")
	machine_id_index_task_events = schema.getFieldNoByContent("task_events" , "machine ID")
	event_type_index_task_events = schema.getFieldNoByContent("task_events" , "event type")
	#Get all the SCHEDULED tasks events and how many machines they are running on
	task_distribution = task_events.filter(lambda x: x[event_type_index_task_events] == '1').map(lambda x: (x[job_id_index_task_events], x[machine_id_index_task_events])).groupByKey()
	#Get all jobs running on more than one machine only obtain the ones running on more than one machine
	final = task_distribution.filter(lambda x: len(x[1]) > 1).mapValues(lambda x: len(set(x)))
	
	#Get how many jobs are each distributed to X machines. 
	#Example (1,5) 5 jobs are running on 1 machine. (X,Y) Y jobs are each running on X machines
	job_count = final.map(lambda x: (x[1] , 1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x: x)

	print(job_count.collect())

def getTaskConsumption():
	task_index_index_task_usage = schema.getFieldNoByContent("task_usage", "task index")
	job_id_index_task_usage = schema.getFieldNoByContent("task_usage", "job ID")
	max_cpu_usage_index_task_usage = schema.getFieldNoByContent("task_usage" , "maximum CPU rate")
	avg_cpu_usage_index_task_usage = schema.getFieldNoByContent("task_usage" , "CPU rate")
	mem_usage_index_task_usage = schema.getFieldNoByContent("task_usage", "canonical memory usage")

	task_index_index_task_events = schema.getFieldNoByContent("task_usage", "task index")
	cpu_request_index_task_events = schema.getFieldNoByContent("task_events", "CPU request")
	memory_request_index_task_events = schema.getFieldNoByContent("task_events", "memory request")
	job_id_index_task_events = schema.getFieldNoByContent("task_events" , "job ID")

	task_request_details = task_events.map(lambda x: ((x[task_index_index_task_events] , x[job_id_index_task_events]) , {"CPU REQ": x[cpu_request_index_task_events], "MEM_REQ" : x[memory_request_index_task_events]}))
	task_usage_details = task_usage.map(lambda x: ((x[task_index_index_task_usage], x[job_id_index_task_usage]), {"MAX CPU USAGE": x[max_cpu_usage_index_task_usage], "AVG CPU": x[avg_cpu_usage_index_task_usage],"MEM USAGE": x[mem_usage_index_task_usage]}))
	
	# joined_data = task_request_details.join(task_usage_details)
	# print(joined_data.collect()[0:10])


	


if __name__ == "__main__":
	sc = None
	schema = None
	initiateSpark()
	start = time.time()
	schema = Schema(sc , "./data/schema.csv")

	
	task_events = loadEvents("./data/task_events/part-00000-of-00500.csv")
	task_events.cache()
	machine_events = loadEvents("./data/machine_events/part-00000-of-00001.csv")
	job_events = loadEvents("./data/job_events/part-00000-of-00500.csv")
	task_usage = loadEvents("./data/task_usage/part-00000-of-00500.csv")



	# getCPUDistribution()
	# computePowerLost()
	# getSchedClassDistribution()
	getClassEvictProbability()
	# getTasksRunningOnMachines()
	# getTaskConsumption()

    
