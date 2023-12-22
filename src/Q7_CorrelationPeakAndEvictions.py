from collections import defaultdict
import time
from matplotlib import pyplot as plt
from schema import Schema
from spark_connection import SparkConnection


machine_events = None
task_usage = None
task_events = None


def getEvictionAndResourceUsagePercentageInIntervals(machine_resouce_value: float, intervals_with_usage: list[tuple[int, int, float]], evictions: list[int]) -> list[tuple[float,int]]: 
    result = []
    evictions_pointer = 0
    
    for start, end, value in intervals_with_usage:
        percent_consumption = 0
        num_evictions = 0
        evictions_len = len(evictions)
        
        while evictions_pointer < evictions_len and evictions[evictions_pointer] < end:
            if start <= evictions[evictions_pointer] < end:
                num_evictions += 1
                evictions_pointer += 1
            else:
                break
        
        percent_consumption = value / machine_resouce_value
        
        #result.append((start, end, percent_consumption, num_evictions))
        result.append((percent_consumption, num_evictions))
    
    return result

def mergeTaskUsageResourceIntervals(lst): 
    '''
        parameters:
            lst : list[tuple[int,int,float]]) 
        returns:
            list[tuple[int,int,float]] 
    '''
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

def helper_getCorrelationPeakAndEvictions(resource_name : str, task_usage_resource_index : int, machine_events_resource_index : int, show_graph : bool):
    
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
    filtered_machine_events = machine_events.filter(lambda e: e[machine_events_machine_id_index]!='' and e[machine_events_resource_index]!='').map(lambda e: (e[machine_events_machine_id_index],float(e[machine_events_resource_index])))

    # get (machine_id, average_resource_available)
    avg_resource_available_per_machine = filtered_machine_events.aggregateByKey((0,0),lambda a,b: (a[0] + b, a[1] + 1),lambda a,b: (a[0] + b[0],a[1] + b[1]))
    avg_resource_available_per_machine = avg_resource_available_per_machine.mapValues(lambda x: float(x[0])/x[1])
    
    # GRoup by machine (machine_id -> (start, end, resource_usage) )
    filtered_task_usage = task_usage.filter(lambda e: e[task_usage_machine_id_index]!='' and e[task_usage_start_time_index]!='' and e[task_usage_end_time_index]!='' and e[task_usage_resource_index]!='')
    filtered_task_usage = filtered_task_usage.map(lambda e: (e[task_usage_machine_id_index],(int(e[task_usage_start_time_index]),int(e[task_usage_end_time_index]),float(e[task_usage_resource_index]))))
    task_resource_usage_per_machine = filtered_task_usage.groupByKey()
    
    # sort tasks by start_time -> end_time -> resource_usage
    sorted_task_resource_usage_per_machine = task_resource_usage_per_machine.mapValues(lambda x : sorted(x))

    # merge intervals to get disjoint intervals with the sum of reource usage for overlapping intervals
    # example  [(1,3,1),(2,4,1)] as input will return [(1,2,1), (2,3,2), (3,4,1)]
    merged_resource_usage_per_machine = sorted_task_resource_usage_per_machine.mapValues(lambda x: mergeTaskUsageResourceIntervals(x))

    # Group by machine (machine -> List<time>) : Get time where tasks where evicted on a machine
    evicted_task_events = task_events.filter(lambda e: e[task_events_machine_id_index]!='' and e[task_events_timestamp_index]!='' and e[task_events_event_type_index]==EVICT).map(lambda e: (e[task_events_machine_id_index],int(e[task_events_timestamp_index])))
    sorted_evicted_task_events_per_machine = evicted_task_events.groupByKey().mapValues(lambda x: sorted(x))
    
    # Join all the information (machine -> (machine_resouces, List<(start,end, resource_usage)>, List<time_of_evictions>))
    availableReources_resourceUsageOverTime_evictions_per_machine = avg_resource_available_per_machine.join(merged_resource_usage_per_machine).join(sorted_evicted_task_events_per_machine).mapValues(lambda x: (x[0][0],x[0][1],x[1]))
    
    # Get  (machine_id -> (percentage_resource_usage, number_of_evictions))
    percent_resource_usage_and_num_evictions_per_machine_over_time = availableReources_resourceUsageOverTime_evictions_per_machine.mapValues(lambda x: getEvictionAndResourceUsagePercentageInIntervals(x[0],x[1],x[2]))

    # Flatten data: Get (percentage_resource_usage, number_of_evictions)
    percent_resouce_usage_and_evictions = percent_resource_usage_and_num_evictions_per_machine_over_time.flatMapValues(lambda x: x).map(lambda x: x[1])

    # Reduce by percentage_resource_usage and round to 3rd decimal
    # Get (percentage_resource_usage, number_of_evictions)
    evictions_per_resource_usage_percentage = percent_resouce_usage_and_evictions.map(lambda x: (round(x[0],3),x[1])).reduceByKey(lambda a,b: a+b).collect()

    if show_graph:
        
        sorted_data = sorted(evictions_per_resource_usage_percentage)
        resource_percent_usage, evictions = zip(*sorted_data)

        # Calculate cumulative frequency
        cumulative_freq = [evictions[0]]
        for i in range(1, len(evictions)):
            cumulative_freq.append(cumulative_freq[i-1] + evictions[i])

        # Plotting the increasing cumulative frequency graph
        plt.plot(resource_percent_usage, cumulative_freq, marker='o',markersize=1, label='Cumulative Frequency')
        
        # plt.plot([point[0] for point in sorted_data], [point[1] for point in sorted_data], marker='o', markersize=1)
        plt.xlabel(f'{resource_name} usage percentage')
        plt.ylabel('Cumulative evictions')
        plt.title('Increasing Cumulative Evictions Plot')
        plt.grid(True)
        plt.legend()
        plt.show()


def getCorrelationPeakAndEvictions(schema):
    print("an we observe correlations between peaks of high resource consumption on some machines and task eviction events?")
    print("-------------------------------------")
    start = time.time()

    task_usage_cpu_index = schema.getFieldNoByContent("task_usage" , "CPU rate")
    machine_events_cpu_available_index = schema.getFieldNoByContent("machine_events", "CPUs")
    helper_getCorrelationPeakAndEvictions("CPU", task_usage_cpu_index, machine_events_cpu_available_index, False)
    
    task_usage_mem_index = schema.getFieldNoByContent("task_usage", "canonical memory usage")
    machine_events_mem_available_index = schema.getFieldNoByContent("machine_events", "Memory")
    helper_getCorrelationPeakAndEvictions("Memory", task_usage_mem_index, machine_events_mem_available_index, False)
    print(f"TIME {time.time() - start}")

def run(conn, schema, file_path):
    global task_usage , machine_events , task_events
    task_events = conn.loadData(file_path+"/task_events/*.csv")
    machine_events = conn.loadData(file_path+"/machine_events/*.csv")
    task_usage = conn.loadData(file_path+"/task_usage/*.csv")
    getCorrelationPeakAndEvictions(schema)

if __name__ == "__main__":
    is_remote = False
    conn = SparkConnection()
    sc = conn.sc
    file_path = "../data/" if not is_remote else "gs://large-data/data"
    schema = Schema(conn , file_path+"/schema.csv")

    run(conn, schema, file_path)
