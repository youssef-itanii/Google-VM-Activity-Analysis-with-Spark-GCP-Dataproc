import sys
from schema import Schema
from spark_connection import SparkConnection
from storage_handler import StorageHandler
import util

@util.execTime
def getClassEvictProbability(schema , task_events):

    print("Do tasks with a low scheduling class have a higher probability of being evicted?")
    print("-------------------------------------")
    
    EVICT = '2'

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
    storage_conn.store({"Q4_class_eviction_probabiltiy" : eviction_probability})



def run(conn, schema, file_path):
    task_events = conn.loadData(file_path+"/task_events/*.csv")
    getClassEvictProbability(schema , task_events)

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
