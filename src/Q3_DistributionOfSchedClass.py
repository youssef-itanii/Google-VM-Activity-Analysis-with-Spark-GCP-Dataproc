import sys
from schema import Schema
from spark_connection import SparkConnection
from storage_handler import StorageHandler
import util

@util.execTime
def getSchedClassDistribution(conn , schema , task_events , job_events):
    print(" What is the distribution of the number of jobs/tasks per scheduling class?")
    print("-------------------------------------")

    sched_class_index_task_events = schema.getFieldNoByContent("task_events" , "scheduling class")
    sched_class_index_job_events = schema.getFieldNoByContent("job_events" , "scheduling class")
    
    task_distribution = task_events.map(lambda x: (x[sched_class_index_task_events] , 1)).reduceByKey(lambda x,y:x+y).collect()

    job_distribution = job_events.map(lambda x: (x[sched_class_index_job_events] , 1)).reduceByKey(lambda x,y:x+y).collect()


    task_distribution.sort(key=lambda x: x[0])
    job_distribution.sort(key=lambda x: x[0])

    storage_conn.store({"Q3_Task_Job_Dist" : {"Task_distribution" :task_distribution , "Job_distrubtion": job_distribution}})




def run(conn, schema, file_path):
    task_events = conn.loadData(file_path+"/task_events/*.csv")
    job_events = conn.loadData(file_path+"/job_events/*.csv")
    getSchedClassDistribution(conn , schema , task_events, job_events)

if __name__ == "__main__":
    is_remote = False
    try:
        is_remote = True if sys.argv[1] == '1' else False
    except IndexError:
        is_remote = False
    conn = SparkConnection()
    sc = conn.sc
    storage_conn = StorageHandler()
    file_path = "../data/" if not is_remote else StorageHandler.path_to_data
    schema = Schema(conn , file_path+"/schema.csv")
    run(conn, schema, file_path)
