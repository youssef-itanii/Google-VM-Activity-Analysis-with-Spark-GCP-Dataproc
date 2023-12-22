import time
from schema import Schema
from spark_connection import SparkConnection

def helper_getTimeUntilRescheduleAndIfMachineChanged(lst : list[tuple[int,str,str]]) -> list[tuple[int,int]]:
    res = [] 
    for i in range(len(lst) - 1):
        if lst[i][1] != lst[i + 1][1]: # event_type_changed
            machine_changed = 1 if lst[i][2] != lst[i + 1][2] else 0
            res.append((lst[i + 1][0] - lst[i][0],machine_changed))
    return res


def getFailureRecovery(schema , task_events):
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

def run(conn, schema, file_path):
    task_events = conn.loadData(file_path+"/task_events/*.csv")
    getFailureRecovery(schema , task_events)

if __name__ == "__main__":
    is_remote = False
    conn = SparkConnection()
    sc = conn.sc
    file_path = "../data/" if not is_remote else "gs://large-data/data"
    schema = Schema(conn , file_path+"/schema.csv")

    run(conn, schema, file_path)
