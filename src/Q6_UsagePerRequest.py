import math
import time

from matplotlib import pyplot as plt
from schema import Schema
from spark_connection import SparkConnection



def helper_getResourceUsagePerRequest(resourceName : str, resourceUsageIndex : int, resourceRequestIndex : int, task_events , task_usage ,is_remote : bool):

    task_events_task_index_index = schema.getFieldNoByContent("task_events", "task index")
    task_events_job_id_index = schema.getFieldNoByContent("task_events" , "job ID")

    task_usage_task_index_index = schema.getFieldNoByContent("task_usage", "task index")
    task_usage_job_id_index = schema.getFieldNoByContent("task_usage", "job ID")

    # filter and map ((job_id,task_id) , (requested_resource))
    filtered_task_events = task_events.filter(lambda e: e[resourceRequestIndex]!='')
    requested_resources_per_task = filtered_task_events.map(lambda e : ((e[task_events_job_id_index],e[task_events_task_index_index]),float(e[resourceRequestIndex])))
    
    # aggregate and get average resources requested by task
    # (job_id,task_id) -> avg_requested_resource
    avg_requested_resources_per_task = requested_resources_per_task.aggregateByKey((0,0),lambda a,b: (a[0] + b, a[1] + 1),lambda a,b: (a[0] + b[0],a[1] + b[1]))
    avg_requested_resources_per_task = avg_requested_resources_per_task.mapValues(lambda x: float(x[0])/x[1])
   
    # filter and map ((job_id,task_id) , (used_resource))
    filtered_task_usage = task_usage.filter(lambda e: e[resourceUsageIndex]!='')
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

    if not is_remote:
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


def getResourceUsagePerRequest(schema , task_usage, task_events, is_remote):
    print("Are the tasks that request the more resources the one that consume the more resources?")
    print("-------------------------------------")
    start = time.time()
    task_usage_cpu_index = schema.getFieldNoByContent("task_usage" , "CPU rate")
    task_events_cpu_request_index = schema.getFieldNoByContent("task_events", "CPU request")
    helper_getResourceUsagePerRequest("CPU", task_usage_cpu_index, task_events_cpu_request_index, task_events, task_usage, is_remote)
    
    task_usage_mem_index = schema.getFieldNoByContent("task_usage", "canonical memory usage")
    task_events_mem_request_index = schema.getFieldNoByContent("task_events", "memory request")
    helper_getResourceUsagePerRequest("Memory", task_usage_mem_index, task_events_mem_request_index,task_events, task_usage, is_remote)

    task_usage_disk_index = schema.getFieldNoByContent("task_usage", "local disk space usage")
    task_events_disk_request_index = schema.getFieldNoByContent("task_events", "disk space request")
    helper_getResourceUsagePerRequest("Disk", task_usage_disk_index, task_events_disk_request_index, task_events, task_usage, is_remote)
    print(f"TIME: {time.time() -  start}")


def run(conn, schema, file_path ,is_remote):
    task_events = conn.loadData(file_path+"/task_events/*.csv")
    task_usage = conn.loadData(file_path+"/task_usage/*.csv")

    task_events_task_index_index = schema.getFieldNoByContent("task_events", "task index")
    task_events_job_id_index = schema.getFieldNoByContent("task_events" , "job ID")

    task_usage_task_index_index = schema.getFieldNoByContent("task_usage", "task index")
    task_usage_job_id_index = schema.getFieldNoByContent("task_usage", "job ID")

    # filter and map ((job_id,task_id) , (requested_resource))
    filtered_task_events = task_events.filter(lambda e: e[task_events_job_id_index]!='' and e[task_events_task_index_index]!='')
    
   
    # filter and map ((job_id,task_id) , (used_resource))
    filtered_task_usage = task_usage.filter(lambda e: e[task_usage_job_id_index]!='' and e[task_usage_task_index_index]!='')



    filtered_task_usage.cache()
    filtered_task_events.cache()
    getResourceUsagePerRequest(schema ,filtered_task_usage, filtered_task_events , is_remote)

if __name__ == "__main__":
    is_remote = False
    conn = SparkConnection("local[8]")
    sc = conn.sc
    file_path = "../data/" if not is_remote else "gs://large-data/data"
    schema = Schema(conn , file_path+"/schema.csv")

    run(conn, schema, file_path , is_remote)