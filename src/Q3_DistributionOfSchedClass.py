from schema import Schema
from spark_connection import SparkConnection
import time


def getSchedClassDistribution(conn , schema , task_events , job_events):

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


    print( {"Job_Task_distrubtion" : {"time": time.time() - start  , "val": {"Task_distribution" :task_distribution , "Job_distrubtion": job_distribution}}})



def run(conn, schema, file_path):
    task_events = conn.loadData(file_path+"/task_events/*.csv")
    job_events = conn.loadData(file_path+"/job_events/*.csv")
    getSchedClassDistribution(conn , schema , task_events, job_events)

if __name__ == "__main__":
    is_remote = True
    conn = SparkConnection()
    sc = conn.sc
    file_path = "../data/" if not is_remote else "gs://large-data/data"
    schema = Schema(conn , file_path+"/schema.csv")

    run(conn, schema, file_path)
