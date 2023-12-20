import sys
from util import *
from pyspark import SparkContext
import time
from schema import Schema
import matplotlib.pyplot as plt
import json
import numpy as np
import math
from collections import defaultdict


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

    print(" What is the distribution of the number of jobs/tasks per scheduling class?")
    print("-------------------------------------")

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

    print("Do tasks with a low scheduling class have a higher probability of being evicted?")
    print("-------------------------------------")
    
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


def getNumberOfMachinePerJobTasks():

    print("In general, do tasks from the same job run on the same machine?")
    print("-------------------------------------")

    SCHEDULE_EVENT = '1'

    start = time.time()
    job_id_index = schema.getFieldNoByContent("task_events" , "job ID")
    machine_id_index = schema.getFieldNoByContent("task_events" , "machine ID")
    event_type_index = schema.getFieldNoByContent("task_events" , "event type")

    #Get all the SCHEDULE tasks events and how many machines they are running on
    schedule_events = task_events.filter(lambda x: x[event_type_index] == SCHEDULE_EVENT)
    
    #group by / job_id -> List<(machine_id)>
    machine_ids_per_job_id = schedule_events.map(lambda x: (x[job_id_index], x[machine_id_index])).groupByKey()
    
    # get jobs where there tasks are scheduled more than once 
    non_single_task_jobs = machine_ids_per_job_id.filter(lambda x: len(x[1]) > 1)

    # job_id -> number_of_machine_for_job_tasks 
    number_of_machines_per_job = non_single_task_jobs.mapValues(lambda x: len(set(x)))
    
    #Get how many jobs are each distributed to X machines. 
    #Example (1,5) 5 jobs are running on 1 machine. (X,Y) Y jobs are each running on X machines
    job_count = number_of_machines_per_job.map(lambda x: (x[1] , 1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x: x)
    end = time.time() - start
    print(job_count.collect())

def helper_getResourceUsagePerRequest(resourceName : str, resourceUsageIndex : int, resourceRequestIndex : int, showGraph : bool):

    task_events_task_index_index = schema.getFieldNoByContent("task_events", "task index")
    task_events_job_id_index = schema.getFieldNoByContent("task_events" , "job ID")

    task_usage_task_index_index = schema.getFieldNoByContent("task_usage", "task index")
    task_usage_job_id_index = schema.getFieldNoByContent("task_usage", "job ID")

    # filter and map ((job_id,task_id) , (requested_resource))
    filtered_task_events = task_events.filter(lambda e: e[task_events_job_id_index]!='' and e[task_events_task_index_index]!='' and e[resourceRequestIndex]!='')
    requested_resources_per_task = filtered_task_events.map(lambda e : ((e[task_events_job_id_index],e[task_events_task_index_index]),float(e[resourceRequestIndex])))
    
    # aggregate and get average resources requested by task
    # (job_id,task_id) -> avg_requested_resource
    avg_requested_resources_per_task = requested_resources_per_task.aggregateByKey((0,0),lambda a,b: (a[0] + b, a[1] + 1),lambda a,b: (a[0] + b[0],a[1] + b[1]))
    avg_requested_resources_per_task = avg_requested_resources_per_task.mapValues(lambda x: float(x[0])/x[1])
   
    # filter and map ((job_id,task_id) , (used_resource))
    filtered_task_usage = task_usage.filter(lambda e: e[task_usage_job_id_index]!='' and e[task_usage_task_index_index]!='' and e[resourceUsageIndex]!='')
    used_resources_per_task = filtered_task_usage.map(lambda e : ((e[task_usage_job_id_index],e[task_usage_task_index_index]),float(e[resourceUsageIndex])))
    
    # aggregate and get average resources used by task
    # (job_id,task_id) -> avg_used_resource
    avg_used_resources_per_task = used_resources_per_task.aggregateByKey((0,0),lambda a,b: (a[0] + b, a[1] + 1),lambda a,b: (a[0] + b[0],a[1] + b[1]))
    avg_used_resources_per_task = avg_used_resources_per_task.mapValues(lambda x: float(x[0])/x[1])
    
    # (job_id,task_id) -> (avg_requested_resource, avg_used_resource)
    resource_per_task = avg_requested_resources_per_task.join(avg_used_resources_per_task)
    
    # map (avg_requested_resource, avg_used_resource)
    resource_usage_per_request = resource_per_task.map(lambda x: (x[1][0],x[1][1]))

    # calculate average and std_dev using variance = mean(x^2) - mean(x)^2
    # avg_requested_resource -> (avg_used_resource,std_dev_used_resource)
    stats_resource_usage_per_request = resource_usage_per_request.aggregateByKey((0,0,0),lambda a,b: (a[0] + b, a[1] + 1, a[2] + b*b),lambda a,b: (a[0] + b[0],a[1] + b[1],a[2] + b[2]))
    stats_resource_usage_per_request = stats_resource_usage_per_request.mapValues(lambda x: (x[0]/x[1], math.sqrt(abs((x[2]/x[1]) - (x[0]/x[1])**2))))
    
    # sort and flatten key-val to tuple (avg_requested_resource, avg_used_resource, std_dev_used_resource) 
    stats_resource_usage_per_request = stats_resource_usage_per_request.sortByKey().map(lambda x: (x[0],x[1][0],x[1][1])).collect()
    
    values, averages, std_devs = zip(*stats_resource_usage_per_request)

    if showGraph:
        # Plotting
        plt.figure(figsize=(8, 6))
        # TO plot with standard dev
        #plt.errorbar(values, averages, yerr=std_devs, fmt='o', capsize=2, markersize=1)
        plt.errorbar(values, averages, fmt='o', capsize=2, markersize=1)
        plt.plot(values, values, linestyle='--', color='red')  # Plot y = x line
        plt.xlabel(f'Requested {resourceName}')
        plt.ylabel(f'Used {resourceName}')
        plt.title(f'Variation of {resourceName} used compared to {resourceName} requested')
        plt.grid(True)
        plt.show()


def getResourceUsagePerRequest():
    print("Are the tasks that request the more resources the one that consume the more resources?")
    print("-------------------------------------")

    task_usage_cpu_index = schema.getFieldNoByContent("task_usage" , "CPU rate")
    task_events_cpu_request_index = schema.getFieldNoByContent("task_events", "CPU request")
    helper_getResourceUsagePerRequest("CPU", task_usage_cpu_index, task_events_cpu_request_index, True)
    
    task_usage_mem_index = schema.getFieldNoByContent("task_usage", "canonical memory usage")
    task_events_mem_request_index = schema.getFieldNoByContent("task_events", "memory request")
    helper_getResourceUsagePerRequest("Memory", task_usage_mem_index, task_events_mem_request_index, True)

    task_usage_disk_index = schema.getFieldNoByContent("task_usage", "local disk space usage")
    task_events_disk_request_index = schema.getFieldNoByContent("task_events", "disk space request")
    helper_getResourceUsagePerRequest("Disk", task_usage_disk_index, task_events_disk_request_index, True)
    

# List<(start, end, resource_usage)> -> return the same type as input where intervals are disjoint
# and the reousrce_usage is summed for overlapping intervals
def helper_mergeTaskUsageResourceIntervals(lst : list[tuple[int,int,float]]) -> list[tuple[int,int,float]]  : 
    if not lst:
        return []

    # Create a sorted dictionary to store time -> value
    time_dict = defaultdict(float)

    # Accumulate values in the dictionary based on start and end times
    for start, end, value in lst:
        time_dict[start] += value
        time_dict[end] -= value

    # Iterate through the dictionary to calculate cumulative values
    cumulative = 0
    result = []
    prev_time = None

    for cur_time in sorted(time_dict.keys()):
        if prev_time is not None:
            result.append((prev_time, cur_time, cumulative))

        cumulative += time_dict[cur_time]
        prev_time = cur_time

    return result

# returns list (resouce_percentage_usage, number_of_evictions)
def helper_getEvictionAndResourceUsagePercentageInIntervals(machineResouceValue: float, intervalsWithUsage: list[tuple[int, int, float]], evictions: list[int]) -> list[tuple[float,int]]: 
    result = []
    evictions_pointer = 0
    
    for start, end, value in intervalsWithUsage:
        percent_consumption = 0
        num_evictions = 0
        evictions_len = len(evictions)
        
        while evictions_pointer < evictions_len and evictions[evictions_pointer] < end:
            if start <= evictions[evictions_pointer] < end:
                num_evictions += 1
                evictions_pointer += 1
            else:
                break
        
        percent_consumption = value / machineResouceValue
        
        #result.append((start, end, percent_consumption, num_evictions))
        result.append((percent_consumption, num_evictions))
    
    return result

def helper_getCorrelationPeakAndEvictions(resourceName : str, taskUsageResourceIndex : int, machineEventsResourceIndex : int, showGraph : bool):
    
    # get indices needed
    machine_events_machine_id_index  = schema.getFieldNoByContent("machine_events", "machine ID")

    task_usage_machine_id_index = schema.getFieldNoByContent("task_usage", "machine ID")
    task_usage_start_time_index = schema.getFieldNoByContent("task_usage", "start time")
    task_usage_end_time_index = schema.getFieldNoByContent("task_usage", "end time")    

    task_events_machine_id_index = schema.getFieldNoByContent("task_events", "machine ID")
    task_events_timestamp_index = schema.getFieldNoByContent("task_events", "time")
    task_events_event_type_index = schema.getFieldNoByContent("task_events", "event type")
    EVICT = '2'

    # Get (machineId, resouceAvailabe)
    filtered_machine_events = machine_events.filter(lambda e: e[machine_events_machine_id_index]!='' and e[machineEventsResourceIndex]!='').map(lambda e: (e[machine_events_machine_id_index],float(e[machineEventsResourceIndex])))

    # get (machine_id, average_resource_available)
    avg_resource_available_per_machine = filtered_machine_events.aggregateByKey((0,0),lambda a,b: (a[0] + b, a[1] + 1),lambda a,b: (a[0] + b[0],a[1] + b[1]))
    avg_resource_available_per_machine = avg_resource_available_per_machine.mapValues(lambda x: float(x[0])/x[1])
    
    # GRoup by machine (machine_id -> (start, end, resource_usage) )
    filtered_task_usage = task_usage.filter(lambda e: e[task_usage_machine_id_index]!='' and e[task_usage_start_time_index]!='' and e[task_usage_end_time_index]!='' and e[taskUsageResourceIndex]!='')
    filtered_task_usage = filtered_task_usage.map(lambda e: (e[task_usage_machine_id_index],(int(e[task_usage_start_time_index]),int(e[task_usage_end_time_index]),float(e[taskUsageResourceIndex]))))
    task_resource_usage_per_machine = filtered_task_usage.groupByKey()
    
    # sort tasks by start_time -> end_time -> resource_usage
    sorted_task_resource_usage_per_machine = task_resource_usage_per_machine.mapValues(lambda x : sorted(x))

    # merge intervals to get disjoint intervals with the sum of reource usage for overlapping intervals
    # example  [(1,3,1),(2,4,1)] as input will return [(1,2,1), (2,3,2), (3,4,1)]
    merged_resource_usage_per_machine = sorted_task_resource_usage_per_machine.mapValues(lambda x: helper_mergeTaskUsageResourceIntervals(x))

    # Group by machine (machine -> List<time>) : Get time where tasks where evicted on a machine
    evicted_task_events = task_events.filter(lambda e: e[task_events_machine_id_index]!='' and e[task_events_timestamp_index]!='' and e[task_events_event_type_index]==EVICT).map(lambda e: (e[task_events_machine_id_index],int(e[task_events_timestamp_index])))
    sorted_evicted_task_events_per_machine = evicted_task_events.groupByKey().mapValues(lambda x: sorted(x))
    
    # Join all the information (machine -> (machine_resouces, List<(start,end, resource_usage)>, List<time_of_evictions>))
    availableReources_resourceUsageOverTime_evictions_per_machine = avg_resource_available_per_machine.join(merged_resource_usage_per_machine).join(sorted_evicted_task_events_per_machine).mapValues(lambda x: (x[0][0],x[0][1],x[1]))
    
    # Get  (machine_id -> (percentage_resource_usage, number_of_evictions))
    percent_resource_usage_and_num_evictions_per_machine_over_time = availableReources_resourceUsageOverTime_evictions_per_machine.mapValues(lambda x: helper_getEvictionAndResourceUsagePercentageInIntervals(x[0],x[1],x[2]))

    # Flatten data: Get (percentage_resource_usage, number_of_evictions)
    percent_resouce_usage_and_evictions = percent_resource_usage_and_num_evictions_per_machine_over_time.flatMapValues(lambda x: x).map(lambda x: x[1])

    # Reduce by percentage_resource_usage and round to 3rd decimal
    # Get (percentage_resource_usage, number_of_evictions)
    evictions_per_resource_usage_percentage = percent_resouce_usage_and_evictions.map(lambda x: (round(x[0],3),x[1])).reduceByKey(lambda a,b: a+b).collect()

    if showGraph:
        
        sorted_data = sorted(evictions_per_resource_usage_percentage)
        resource_percent_usage, evictions = zip(*sorted_data)

        # Calculate cumulative frequency
        cumulative_freq = [evictions[0]]
        for i in range(1, len(evictions)):
            cumulative_freq.append(cumulative_freq[i-1] + evictions[i])

        # Plotting the increasing cumulative frequency graph
        plt.plot(resource_percent_usage, cumulative_freq, marker='o',markersize=1, label='Cumulative Frequency')
        
        # plt.plot([point[0] for point in sorted_data], [point[1] for point in sorted_data], marker='o', markersize=1)
        plt.xlabel(f'{resourceName} usage percentage')
        plt.ylabel('Cumulative evictions')
        plt.title('Increasing Cumulative Evictions Plot')
        plt.grid(True)
        plt.legend()
        plt.show()

def getCorrelationPeakAndEvictions():
    print("an we observe correlations between peaks of high resource consumption on some machines and task eviction events?")
    print("-------------------------------------")
    

    task_usage_cpu_index = schema.getFieldNoByContent("task_usage" , "CPU rate")
    machine_events_cpu_available_index = schema.getFieldNoByContent("machine_events", "CPUs")
    helper_getCorrelationPeakAndEvictions("CPU", task_usage_cpu_index, machine_events_cpu_available_index, True)
    
    task_usage_mem_index = schema.getFieldNoByContent("task_usage", "canonical memory usage")
    machine_events_mem_available_index = schema.getFieldNoByContent("machine_events", "Memory")
    helper_getCorrelationPeakAndEvictions("Memory", task_usage_mem_index, machine_events_mem_available_index, True)

# takes list of (timestamp,event_type,machine_id) and return for how much time it took for each fail and reschedule to
# take place and if the machine changed or not
def helper_getTimeUntilRescheduleAndIfMachineChanged(lst : list[tuple[int,str,str]]) -> list[tuple[int,int]]:
    res = [] 
    for i in range(len(lst) - 1):
        if lst[i][1] != lst[i + 1][1]: # event_type_changed
            machine_changed = 1 if lst[i][2] != lst[i + 1][2] else 0
            res.append((lst[i + 1][0] - lst[i][0],machine_changed))
    return res


def getFailureRecovery():
    print("How much time does it usually takes a task to be rescheduled after failure? Are they rescheduled on the same machine or not?")
    print("-------------------------------------")
    
    SCHEDULE = '1'
    FAIL = '3'

    job_id_index = schema.getFieldNoByContent("task_events" , "job ID") 
    task_index_index = schema.getFieldNoByContent("task_events" , "task index")
    timestamp_index = schema.getFieldNoByContent("task_events", "time")
    event_type_index = schema.getFieldNoByContent("task_events", "event type")
    machine_id_index = schema.getFieldNoByContent("task_events", "machine ID")

    # filter task events
    # Get ((job_id,task_index),(timestamp,event_type,machine_id))
    filtered_task_events = task_events.filter(lambda e: e[job_id_index]!='' and e[task_index_index]!='' and e[timestamp_index]!='' and e[event_type_index]!='' and e[machine_id_index]!='')
    filtered_task_events = filtered_task_events.map(lambda e: ((e[job_id_index],e[task_index_index]),(int(e[timestamp_index]),e[event_type_index],e[machine_id_index])))
    
    # filter schedule and fail events
    schedule_and_fail_events = filtered_task_events.filter(lambda e: e[0][1]==SCHEDULE or e[0][1]==FAIL)
    
    # group by task and sort values by timestamp
    # (job_id,task_index) -> (timestamp,event_type,machine_id)
    schedule_and_fail_events_by_task = schedule_and_fail_events.groupByKey().mapValues(lambda x: sorted(x))

    # Get ( time_till_reschedule, machine_changed = 0|1 )
    time_till_reschedule_and_machine_changed = schedule_and_fail_events_by_task.flatMapValues(lambda x: helper_getTimeUntilRescheduleAndIfMachineChanged(x))

    time_till_reschedule = time_till_reschedule_and_machine_changed.map(lambda x: x[1][0]).stats()
    print(time_till_reschedule)

    check_if_machine_changed = time_till_reschedule_and_machine_changed.map(lambda x: x[1][1])

    check_if_machine_changed_count = check_if_machine_changed.count()
    num_machine_changed = check_if_machine_changed.sum()
    print(f'count = {check_if_machine_changed_count}')
    print(f'machine_changed = {num_machine_changed}')
    print(f'Percentage machine changed = {100*float(num_machine_changed)/check_if_machine_changed_count}%')



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
    # getClassEvictProbability()
    # getNumberOfMachinePerJobTasks()
    # getResourceUsagePerRequest()
    # getCorrelationPeakAndEvictions()
    getFailureRecovery()
    
    print(json.dumps(output))
    print("------------------- DONE -----------------");