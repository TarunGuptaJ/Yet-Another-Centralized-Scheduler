import os
import sys
import csv
import json
import copy
import socket
import random
import threading
from threading import Lock
from time import sleep, time

path_to_config = sys.argv[1]
sched_algo = sys.argv[2]

job_log_name = "job_log" + sched_algo + ".csv"
task_log_name = "task_log" + sched_algo + ".csv"

with open(job_log_name,'w+',newline='') as job_log:
    job_log_writer = csv.writer(job_log, delimiter=',', quotechar="'")
    job_log_writer.writerow(['JobId','ExecutionTime','ArrivalTime','CompletionTime'])

with open(task_log_name,'w+',newline='') as task_log:
    task_log_writer = csv.writer(task_log, delimiter=',', quotechar="'")
    task_log_writer.writerow(['TaskId','WorkerId','ExecutionTime','ArrivalTime','Completion'])

# Data structures shared across the master threads
workers_state = dict()
map_jobs_tbd = list()
reduce_jobs_tbd = list()
job_state = dict()

# Locks to access the corresponding structure
workers_state_lock = Lock()
map_jobs_tbd_lock = Lock()
reduce_jobs_tbd_lock = Lock()
job_state_lock = Lock()

# Random scheduling algorithm
# Returns the worker by random choice and sleeps for 0.05s if all are busy
def randomScheduling():
    while True:
        workers_state_lock.acquire()
        choice = random.choice(list(workers_state.keys()))   
        free_slots = workers_state[choice]['slots'] - workers_state[choice]['occupied_slots']
        if(free_slots > 0):
            print(choice)
            workers_state[choice]['occupied_slots'] += 1
            workers_state_lock.release()
            return choice
        workers_state_lock.release()
        sleep(0.05)

# Round Robin scheduling algorithm
# Returns the worker based on round robin scheduling and sleeps for 0.05s if all are busy
roundRobinIndex = 0
def roundRobinScheduling():
    global roundRobinIndex
    while True:
        workers_state_lock.acquire()
        workers_list = list(workers_state.keys())
        choice = list(workers_state.keys())[roundRobinIndex]
        free_slots = workers_state[choice]['slots'] - workers_state[choice]['occupied_slots']
        if(free_slots > 0):
            workers_state[choice]['occupied_slots'] += 1
            roundRobinIndex = (roundRobinIndex + 1) % len(workers_list)
            workers_state_lock.release()
            return choice
        roundRobinIndex = (roundRobinIndex + 1) % len(workers_list)
        workers_state_lock.release() 
        sleep(0.05)

# Least Loaded scheduling algorithm
# Returns the worker with the most free slots and sleeps for 1s if there are no slots
def leastLoadedScheduling():
    while True:
        workers_state_lock.acquire()
        workers_list = list(workers_state.keys())
        max_free_key = max(workers_state,key = lambda x: workers_state[x]['slots'] - workers_state[x]['occupied_slots'])
        max_free = workers_state[max_free_key]
        free_slots = max_free['slots'] - max_free['occupied_slots'] 
        if(free_slots > 0):
            choice = max_free['worker_id']
            workers_state[choice]['occupied_slots'] += 1
            workers_state_lock.release()
            return choice
        workers_state_lock.release()
        sleep(1)

# Fucntion to listen for jobs on port 5000
def listenJob():
    job_socket = socket.socket()
    job_socket.bind(('', 5000))
    job_socket.listen(5)

    while True:
        connection, address = job_socket.accept()
        received = connection.recv(2048)
        arrival_time = time()
        job = json.loads(received)
        
        map_jobs_tbd_lock.acquire()
        job_state_lock.acquire()

        # Adding relevant information to the job_state structure
        job_id = job["job_id"]
        job_state[job_id] = dict()
        job_state[job_id]["arrival_time"] = arrival_time
        job_state[job_id]["map_tasks"] = job["map_tasks"]
        job_state[job_id]['map_completed'] = False
        job_state[job_id]['completed'] = False
       
        for i in range(len(job["map_tasks"])):
            job_state[job_id]["map_tasks"][i]['job_id'] = job_id
        
        job_state[job_id]["reduce_tasks"] = job["reduce_tasks"]

        for i in range(len(job["reduce_tasks"])):
            job_state[job_id]["reduce_tasks"][i]['job_id'] = job_id

        job_state[job_id]["map_tasks_completed"] = list()
        job_state[job_id]["reduce_tasks_completed"] = list()

        # Adding map tasks to the map job pool
        map_jobs_tbd.extend(job_state[job_id]["map_tasks"])

        job_state_lock.release()
        map_jobs_tbd_lock.release()

        connection.close()

# Function to listen for worker responses on port 5001
def listenWorker():
    worker_socket = socket.socket()
    worker_socket.bind(('', 5001))
    worker_socket.listen(5)
    
    while True:
        print("listenWorker")
        connection, address = worker_socket.accept()
        received = connection.recv(2048)
        completed_task = json.loads(received)

        # Decrementing the occupied slots for the worker
        worker_id = completed_task['worker_id']
        workers_state_lock.acquire()
        workers_state[worker_id]['occupied_slots'] -= 1
        print(workers_state[worker_id])
        workers_state_lock.release()


        # Logging task information
        with open(task_log_name,'a',newline='') as task_log:
            task_log_writer = csv.writer(task_log, delimiter=',', quotechar="'")
            execution_time = completed_task['completion_time'] - completed_task['arrival_time_worker']
            
            task_log_writer.writerow([completed_task['task_id'],completed_task['worker_id'],
                                      execution_time,completed_task['arrival_time_worker'],
                                      completed_task['completion_time']])
        
        job_id = completed_task['job_id']
        job_state_lock.acquire()
        
        # Adding completed tasks to the completed list
        if(completed_task['type'] == 'M'):
            job_state[job_id]['map_tasks_completed'].append(completed_task)

        elif(completed_task['type'] == 'R'):
            job_state[job_id]['reduce_tasks_completed'].append(completed_task)

        # Adding reduce tasks if all corresponding map tasks are done
        if(len(job_state[job_id]['map_tasks']) == len(job_state[job_id]['map_tasks_completed']) and job_state[job_id]['map_completed'] == False):
            reduce_jobs_tbd_lock.acquire()
            reduce_jobs_tbd.extend(job_state[job_id]['reduce_tasks'])
            reduce_jobs_tbd_lock.release()
            job_state[job_id]['map_completed'] = True

        # Marking job as completed if all reduce tasks are done
        if(len(job_state[job_id]['reduce_tasks']) == len(job_state[job_id]['reduce_tasks_completed']) and job_state[job_id]['completed'] == False):
            job_state[job_id]["execution_time"] = completed_task['completion_time'] - job_state[job_id]["arrival_time"]
            job_state[job_id]['completed'] = True
            
            # Logging job information
            with open(job_log_name,'a',newline='') as job_log:
                job_log_writer = csv.writer(job_log, delimiter=',', quotechar="'")
                job_log_writer.writerow([job_id,job_state[job_id]["execution_time"],job_state[job_id]['arrival_time'],completed_task['completion_time']])

        job_state_lock.release()
        connection.close()

# Function to get worker id based on algorithm chosen
def assignWorker(task):   
    if(sched_algo == "RANDOM"):
        worker = randomScheduling()
    elif(sched_algo == "RR"):
        worker = roundRobinScheduling()
    elif(sched_algo == "LL"):
        worker = leastLoadedScheduling()
    else:
        exit()

    # Sending job information to the worker
    workers_state_lock.acquire()
    port = workers_state[worker]['port']
    worker_id = workers_state[worker]['worker_id']
    workers_state_lock.release()
    task['worker_id'] = worker_id
    worker_socket = socket.socket()
    worker_socket.connect(('127.0.0.1', port))
    worker_socket.send(json.dumps(task).encode('utf-8'))
    worker_socket.close()

# Assigning workers for each job that has to be done
def scheduleJobs():
    while True:
        print("Waiting for job request.....")
        # Assigning workers for reduce jobs
        reduce_jobs_tbd_lock.acquire()
        global reduce_jobs_tbd
        
        for reduce_job in reduce_jobs_tbd:
            job = reduce_job
            job["type"] = 'R'
            assignWorker(job)
        
        reduce_jobs_tbd = list()
        reduce_jobs_tbd_lock.release()
        # Assigning workers for map jobs
        map_jobs_tbd_lock.acquire()
        global map_jobs_tbd
      
        for map_job in map_jobs_tbd:
            job = map_job
            job["type"] = 'M'
            assignWorker(job)
        
        map_jobs_tbd = list()
        map_jobs_tbd_lock.release()

# Initializing worker state
def initialize():
    workers_state_lock.acquire()

    global workers_state
    config_file = open(path_to_config)
    config = json.load(config_file)
    workers = config["workers"]
    workers_state = {worker["worker_id"] : worker for worker in workers}
    for i in workers_state:
        workers_state[i]["occupied_slots"] = 0
    
    workers_state_lock.release()

initialize()
# Initializing threads
thread_1 = threading.Thread(target = listenJob)
thread_2 = threading.Thread(target = scheduleJobs)
thread_3 = threading.Thread(target = listenWorker)

# Starting threads
thread_1.start()
thread_2.start()
thread_3.start()

# Joining threads
thread_3.join()
thread_2.join()
thread_1.join()