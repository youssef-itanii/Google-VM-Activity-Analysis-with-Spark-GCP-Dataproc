import math
import time
from matplotlib import pyplot as plt
from pyspark.sql import SparkSession
from schema import Schema
from spark_connection import SparkConnection
from pyspark.sql import Row

def convertToFloat(val):
    try:
        return float(val)
    except ValueError:
        return None

def helper_getResourceUsagePerRequest(conn, resourceName, resourceUsageIndex, resourceRequestIndex, task_events_rdd, task_usage_rdd):
    
    task_usage_task_index_index = schema.getFieldNoByContent("task_usage", "task index")
    task_usage_job_id_index = schema.getFieldNoByContent("task_usage", "job ID")
    task_events_task_index_index = schema.getFieldNoByContent("task_events", "task index")
    task_events_job_id_index = schema.getFieldNoByContent("task_events" , "job ID")
   
    #Filter and map ((job_id,task_id) , (requested_resource)) as dataframes
    task_events = conn.createDataFrame(task_events_rdd.map(lambda e: Row(job_id=e[task_events_job_id_index], task_id=e[task_events_task_index_index], requested_resource=convertToFloat(e[resourceRequestIndex]))))
    task_usage = conn.createDataFrame(task_usage_rdd.map(lambda e: Row(job_id=e[task_usage_job_id_index], task_id=e[task_usage_task_index_index], used_resource=convertToFloat(e[resourceUsageIndex]))))

    #Compute averages and join
    avg_requested_resources = task_events.groupBy("job_id", "task_id") \
                                               .avg("requested_resource") \
                                               .withColumnRenamed("avg(requested_resource)", "avg_requested_resource")

    avg_used_resources = task_usage.groupBy("job_id", "task_id") \
                                         .avg("used_resource") \
                                         .withColumnRenamed("avg(used_resource)", "avg_used_resource")

    # (job_id,task_id) -> (avg_requested_resource, avg_used_resource)
    resource_per_task = avg_requested_resources.join(avg_used_resources, ["job_id", "task_id"])

    
    #Calculates average and std_dev using variance = mean(x^2) - mean(x)^2
    #avg_requested_resource -> (avg_used_resource,std_dev_used_resource)
    '''
        1. Create column with x^2 
        2. Group the data by the avg_requested_resources
        3. Get the average used of the group, and mean(x^2)
        4. Rename the columns that were obtained from the aggregation
    '''
    stats = resource_per_task.withColumn('x^2', resource_per_task['avg_used_resource'] ** 2) \
                                  .groupBy('avg_requested_resource') \
                                  .agg({'avg_used_resource': 'avg', 'x^2': 'avg'}) \
                                  .withColumnRenamed('avg(avg_used_resource)', 'mean_used_resource') \
                                  .withColumnRenamed('avg(x^2)', 'mean_squared_used_resource')

    #Calculate average and std_dev using variance = mean(x^2) - mean(x)^2
    #avg_requested_resource -> (avg_used_resource,std_dev_used_resource)
    stats = stats.withColumn('std_dev_used_resource', (stats['mean_squared_used_resource'] - stats['mean_used_resource'] ** 2) ** 0.5)

    plot_data = stats.collect()
    values, averages, std_devs = zip(*[(row['avg_requested_resource'], row['mean_used_resource'], row['std_dev_used_resource']) for row in plot_data])

    if not is_remote:
        plt.figure(figsize=(8, 6))
        # plt.errorbar(values, averages, yerr=std_devs, fmt='o', capsize=2, markersize=1)
        plt.errorbar(values, averages, fmt='o', capsize=2, markersize=1)

        plt.plot(values, values, linestyle='--', color='red')
        plt.xlabel(f'Requested {resourceName}')
        plt.ylabel(f'Used {resourceName}')
        plt.title(f'Variation of {resourceName} used compared to {resourceName} requested')
        plt.grid(True)
        plt.show()

def getResourceUsagePerRequest(conn, schema, task_usage, task_events, is_remote):
    print("Are the tasks that request more resources the ones that consume more resources?")
    print("-------------------------------------")
    start = time.time()

    task_usage_cpu_index = schema.getFieldNoByContent("task_usage" , "CPU rate")
    task_events_cpu_request_index = schema.getFieldNoByContent("task_events", "CPU request")
    helper_getResourceUsagePerRequest(conn, "CPU", task_usage_cpu_index, task_events_cpu_request_index, task_events, task_usage)

    task_usage_mem_index = schema.getFieldNoByContent("task_usage", "canonical memory usage")
    task_events_mem_request_index = schema.getFieldNoByContent("task_events", "memory request")
    helper_getResourceUsagePerRequest(conn , "Memory", task_usage_mem_index, task_events_mem_request_index,task_events, task_usage)

    task_usage_disk_index = schema.getFieldNoByContent("task_usage", "local disk space usage")
    task_events_disk_request_index = schema.getFieldNoByContent("task_events", "disk space request")
    helper_getResourceUsagePerRequest(conn , "Disk", task_usage_disk_index, task_events_disk_request_index, task_events, task_usage)

    print(f"TIME: {time.time() - start}")

def run(conn, schema, file_path, is_remote):
    conn = SparkSession.builder.appName("ResourceUsageOptimization").getOrCreate()

    task_events = conn.loadData(file_path + "/task_events/*.csv")
    task_usage = conn.loadData(file_path + "/task_usage/*.csv")
   
    task_events_task_index_index = schema.getFieldNoByContent("task_events", "task index")
    task_events_job_id_index = schema.getFieldNoByContent("task_events" , "job ID")

    task_usage_task_index_index = schema.getFieldNoByContent("task_usage", "task index")
    task_usage_job_id_index = schema.getFieldNoByContent("task_usage", "job ID")

    #Filter out the empty fields
    filtered_task_events = task_events.filter(lambda e: e[task_events_job_id_index]!='' and e[task_events_task_index_index]!='')
    filtered_task_usage = task_usage.filter(lambda e: e[task_usage_job_id_index]!='' and e[task_usage_task_index_index]!='')


    filtered_task_usage.cache()
    filtered_task_events.cache()


    getResourceUsagePerRequest(conn, schema, filtered_task_usage, filtered_task_events, is_remote)
    conn.stop()

if __name__ == "__main__":
    is_remote = False
    conn = SparkConnection("local[8]")
    sc = conn.sc
    file_path = "../data/" if not is_remote else "gs://large-data/data"
    schema = Schema(conn, file_path + "/schema.csv")

    run(conn, schema, file_path, is_remote)
