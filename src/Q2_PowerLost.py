import json
from schema import Schema
from spark_connection import SparkConnection
import time
import util

def computePowerLost(conn , schema , file_path):
    machine_events = conn.loadData(file_path+"/machine_events/part-00000-of-00001.csv")

    
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



# takes as params list of (timestamp, EVENT_TYPE(ADD|REMOVE|UPDATE), cpu) and maxTime of trace
# returns list of (time_on,time_off,cpu) which represents the time where the machine was on and off
# with the corresponding cpu ratio (A machine resources could get updated)
def helper_computeOnAndOffDuration(lst , maxTime: int):  
# def helper_computeOnAndOffDuration(lst : list[tuple[int,str,float]], maxTime: int):
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


def run(conn, schema, file_path):
    computePowerLost(conn , schema , file_path)

if __name__ == "__main__":
    is_remote = True
    conn = SparkConnection()
    sc = conn.sc
    file_path = "../data/" if not is_remote else "gs://large-data/data"
    schema = Schema(conn , file_path+"/schema.csv")

    run(conn, schema, file_path)
