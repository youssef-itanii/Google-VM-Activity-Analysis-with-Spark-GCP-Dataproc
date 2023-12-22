import json
from schema import Schema
from spark_connection import SparkConnection
import time
import util

def getCPUDistribution(conn , schema , file_path):
    machine_events = conn.loadData(file_path+"/machine_events/part-00000-of-00001.csv")
    
    print("What is the distribution of the machines according to their CPU capacity?")
    print("-------------------------------------")
    start = time.time()
    
    cpu_index = schema.getFieldNoByContent("machine_events", "CPU")
    machine_id_index = schema.getFieldNoByContent("machine_events", "machine ID") 

    machine_cpu_distribution = machine_events.filter(lambda x: x[cpu_index]!='').map(lambda x: (x[machine_id_index], x[cpu_index])).groupByKey()
    res = machine_cpu_distribution.mapValues(lambda x: list(x)[0])
    cpu_distribution = res.map(lambda x: (x[1] , 1)).reduceByKey(lambda x,y:x+y).collect()

    print(util.generateJson("cpu_distribution", {"time": time.time() - start , "val":cpu_distribution}))

    
    print("=========================================================================================\n")

def run(conn, schema, file_path):
    getCPUDistribution(conn , schema , file_path)

if __name__ == "__main__":
    is_remote = True
    conn = SparkConnection()
    sc = conn.sc
    file_path = "../data/" if not is_remote else "gs://large-data/data"
    schema = Schema(conn , file_path+"/schema.csv")
    run(conn, schema, file_path)
