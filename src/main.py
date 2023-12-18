import sys
from util import *
from pyspark import SparkContext
import time
from schema import Schema
import matplotlib.pyplot as plt
import json


def appendToOutput(key, time , value):
    global output
    output[key] = {"time": time , "value":value}

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
    start = time.time()
    
    cpu_index = schema.getFieldNoByContent("machine_events", "CPU")
    machine_id_index = schema.getFieldNoByContent("machine_events", "machine ID") 

    machine_cpu_distribution = machine_events.filter(lambda x: x[cpu_index]!='').map(lambda x: (x[machine_id_index], x[cpu_index])).groupByKey()
    res = machine_cpu_distribution.mapValues(lambda x: list(x)[0])
    cpu_distribution = res.map(lambda x: (x[1] , 1)).reduceByKey(lambda x,y:x+y).collect()
    # plotDistribution(cpu_distribution , "CPU" , "Machines", "Distribution of CPU Across Machines")

    appendToOutput("CPU_Distribution" , time.time() - start  , cpu_distribution)

    
    print("=========================================================================================\n")



    
def isCPUChanging():
    cpu_index = schema.getFieldNoByContent("machine_events", "CPU")
    
    # #Get the index of the machine ID in the task event
    machine_id_index_task_event = int(schema.getFieldNoByContent("machine_events", "machine ID")) 

    distribution = machine_events.filter(lambda x: x[cpu_index]!='').map(lambda x: (x[machine_id_index_task_event], x[cpu_index])).groupByKey()
    final = distribution.mapValues(lambda x: set(x)).filter(lambda x: len(x[1])>1).collect()
    print(final)

# takes as params list of (timestamp, EVENT_TYPE(ADD|REMOVE|UPDATE), cpu) and maxTime of trace
# returns list of (time_on,time_off,cpu) which represents the time where the machine was on and off
# with the corresponding cpu ratio (A machine resources could get updated)
def helper_computeOnAndOffDuration(lst : list[tuple[int,str,float]], maxTime: int):
        ''' 
            Each record has a timestamp, which is in microseconds since 600 seconds before the
            beginning of the trace period, and recorded as a 64 bit integer (i.e., an event 20 seconds
            after the start of the trace would have a timestamp=620s).
        '''
        ADD_EVENT = '0'
        REMOVE_EVENT = '1'
        UPDATE_EVENT = '2'
        res = []
        last_timestamp = 600_000_000
        is_machine_on = True
        time_on = 0
        time_off = 0
        last_cpu = lst[0][2]

        for timestamp,event_type,cpu in lst:

            # special, trace start at time t=600s
            if timestamp < 600_000_000:
                if event_type == REMOVE_EVENT:
                    is_machine_on = False
                else:
                    is_machine_on = True
                continue

            if is_machine_on:
                time_on += timestamp - last_timestamp
            else: 
                time_off += timestamp - last_timestamp
            last_timestamp = timestamp

            if event_type == ADD_EVENT:
                is_machine_on = True
            elif event_type == REMOVE_EVENT:
                is_machine_on = False
            elif event_type == UPDATE_EVENT:
                res.append((time_off,time_on,last_cpu))
                last_cpu = cpu
                time_off = 0
                time_on = 0
            else:
                print('Error: Unexpected machine_event_type')
                
        if is_machine_on:
            time_on += maxTime - last_timestamp
        else: 
            time_off += maxTime - last_timestamp

        if time_off!=0 or time_on!=0:
            res.append((time_off,time_on,last_cpu))

        return res

def computePowerLost():
    
    print("What is the percentage of computational power lost due to maintenance (a machine went offline and reconnected later)?")
    print("-------------------------------------")
    
    
    machine_id_index = schema.getFieldNoByContent("machine_events", "machine ID")
    timestamp_index = schema.getFieldNoByContent("machine_events", "time")
    event_type_index = schema.getFieldNoByContent("machine_events", "event type")
    cpu_index = schema.getFieldNoByContent("machine_events", "CPU")

    # ignore events with missing data
    filtered_machine_events = machine_events.filter(lambda e:  e[timestamp_index]!='' and e[event_type_index]!='' and e[cpu_index]!='')
    
    # map machine_id => (timestamp, event_type, cpu_ratio)
    machine_change_events = filtered_machine_events.map(lambda e: (e[machine_id_index], (int(e[timestamp_index]), e[event_type_index], float(e[cpu_index]))))
    
    # group by machine id / machine_id => List<(timestamp, event_type, cpu_ratio)>
    events_by_machine = machine_change_events.groupByKey()
    
    # sort values by timestamp
    sorted_events_by_machine = events_by_machine.mapValues(lambda x : sorted(x))

    # get the max timestamp in the trace
    max_timestamp = sorted_events_by_machine.max(lambda e: e[1][0])[1][1][0]

    # machineId -> (time_off, time_on, cpu_ratio)
    on_off_durations_with_cpu_per_machine = sorted_events_by_machine.flatMapValues(lambda x: helper_computeOnAndOffDuration(x,max_timestamp))

    # sum of cpu_ratio in (time_off, time_on, cpu_ratio)
    total_cpu = on_off_durations_with_cpu_per_machine.map(lambda pr: pr[1][2]).sum()

    # sum of (time_off / (time_off + time_on)) * cpu_ratio
    computational_loss = on_off_durations_with_cpu_per_machine.map(lambda pr: (float(pr[1][0])/(pr[1][0]+pr[1][1]))* pr[1][2]).sum()
    
    # calculate power loss percentage
    power_loss = 100 * computational_loss / total_cpu
    
    print(f"Percentage of power lost: {power_loss}%")
    print("=========================================================================================\n")


def getSchedClassDistribution():

    start = time.time()
    sched_class_index_task_events = schema.getFieldNoByContent("task_events" , "scheduling class")
    sched_class_index_job_events = schema.getFieldNoByContent("job_events" , "scheduling class")
    
    task_distribution = task_events.map(lambda x: (x[sched_class_index_task_events] , 1)).reduceByKey(lambda x,y:x+y).collect()
    job_distribution = job_events.map(lambda x: (x[sched_class_index_job_events] , 1)).reduceByKey(lambda x,y:x+y).collect()

    task_distribution.sort(key=lambda x: x[0])
    job_distribution.sort(key=lambda x: x[0])
    # plotDistribution(task_distribution , "Class" , "Tasks", "Distribution of Tasks Across Scheduling Classes")
    # plotDistribution(job_distribution , "Class" , "Jobs", "Distribution of Jobs Scheduling Classes")


    appendToOutput("Job_Task_distrubtion" , time.time() - start  , {"Task_distribution" :task_distribution , "Job_distrubtion": job_distribution})



def getClassEvictProbability():
    EVICT = '2'

    start = time.time()
    event_type_index = schema.getFieldNoByContent("task_events", "event type")
    scheduling_class_index = schema.getFieldNoByContent("task_events" , "scheduling class")
    task_index_index = schema.getFieldNoByContent("task_events" , "task index")
    job_id_index = schema.getFieldNoByContent("task_events" , "job ID") 
    
    # note that a task could change its scheduling class
    # map (job_id,task_index,scheduling_class) -> (event_type)
    class_and_event_type_per_task = task_events.map(lambda e: ((e[job_id_index],e[task_index_index],e[scheduling_class_index]),(e[event_type_index])))
    
    # filter where (job_id,task_index,scheduling_class) -> (EVICT)
    evictions_per_task = class_and_event_type_per_task.filter(lambda x: x[1][0] == EVICT).groupByKey()
    
    # get number of tasks evicted per class (scheduling_class -> tasks evicted)
    evicted_tasks_per_class = evictions_per_task.map(lambda pr : (int(pr[0][2]) , 1)).reduceByKey(lambda x,y:x+y).collect()
    evicted_tasks_per_class = dict(evicted_tasks_per_class)
    
    # get number of tasks per class 
    total_tasks_per_class = class_and_event_type_per_task.map(lambda x: (int(x[0][2])  , 1)).reduceByKey(lambda x,y : x+y)

    # get the probability that is evcited per class (num_evicted_tasks/total_num_of_tasks)
    eviction_probability = total_tasks_per_class.map(lambda x: (x[0] , round(evicted_tasks_per_class[x[0]]/x[1],4) ) if x[0] in evicted_tasks_per_class else (x[0] , 0)).sortByKey().collect()
    appendToOutput("Class_Eviction_Probabiltiy" , time.time() - start  , eviction_probability)




def getTasksRunningOnMachines():
    start = time.time()
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
    end = time.time() - start
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
    output = {}
    initiateSpark()
    on_gcp = False
    file_path = "../data/" if not on_gcp else "gs://large-data/data"
    schema = Schema(sc , file_path+"/schema.csv")

    
    task_events = loadEvents(file_path+"/task_events/part-00000-of-00500.csv")
    task_events.cache()
    machine_events = loadEvents(file_path+"/machine_events/part-00000-of-00001.csv")
    job_events = loadEvents(file_path+"/job_events/part-00000-of-00500.csv")
    task_usage = loadEvents(file_path+"/task_usage/part-00000-of-00500.csv")


    print("\n------------------- START -----------------\n");
    # getCPUDistribution()
    # computePowerLost()
    # getSchedClassDistribution()
    getClassEvictProbability()
    # getTasksRunningOnMachines()
    # getTaskConsumption()
    print(json.dumps(output))
    print("------------------- DONE -----------------");