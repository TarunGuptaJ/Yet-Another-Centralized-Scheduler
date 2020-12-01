import sys
import json
import copy
import socket
import threading
from threading import Lock
from time import sleep, time

# Structure to store all tasks to be done
task_tbd = list()
task_tbd_lock = Lock()

port = int(sys.argv[1])
worker_id = int(sys.argv[2])

# Function to listen for tasks from master
def listenMaster():
    job_socket = socket.socket()
    job_socket.bind(('', port))
    job_socket.listen(5)

    while True:
        connection, address = job_socket.accept()
        received = connection.recv(2048)
        arrival_time = time()
        task = json.loads(received)
        # Logging arrival time
        task['arrival_time_worker'] = arrival_time
        task_tbd_lock.acquire()
        task_tbd.append(task)
        task_tbd_lock.release()

# Function to execute the jobs to be done
def executeJobs():
    global task_tbd
    while True:
        sleep(0.05)
        task_tbd_lock.acquire()
        # Sleeping for 1s before reducing durations
        sleep(1)
        task_tbd_new = []
        for i in range(len(task_tbd)):
            task_tbd[i]['duration'] -= 1
            if(task_tbd[i]['duration'] == 0):
                # Sending response to master after task completed
                print("Finished task",task_tbd[i]['task_id'])
                finished_task = task_tbd[i]
                finished_task['completion_time'] = time()
                master_socket = socket.socket()                
                master_socket.connect(('127.0.0.1', 5001))  
                master_socket.send(json.dumps(finished_task).encode('utf-8')) 
                master_socket.close() 
            else:
                # Updating tasks to be done
                task_tbd_new.append(task_tbd[i])
        task_tbd = task_tbd_new
        task_tbd_lock.release()

thread_1 = threading.Thread(target = listenMaster)
thread_2 = threading.Thread(target = executeJobs)

thread_1.start()
thread_2.start()

thread_2.join()
thread_1.join()