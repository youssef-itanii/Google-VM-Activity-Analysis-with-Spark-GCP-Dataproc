import sys
from storage_handler import StorageHandler
import util
from schema import Schema
from spark_connection import SparkConnection

@util.execTime
def getNumberOfMachinePerJobTasks(schema, task_events):

    print("In general, do tasks from the same job run on the same machine?")
    print("-------------------------------------")

    SCHEDULE_EVENT = '1'

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
    storage_conn.store({"Q5_job_distribution": job_count.collect()})

def run(conn, schema, file_path):
    task_events = conn.loadData(file_path+"/task_events/*.csv")
    getNumberOfMachinePerJobTasks(schema , task_events)

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

