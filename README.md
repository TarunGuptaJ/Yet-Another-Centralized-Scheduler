# BD-PROJECT-YACS
YACS - Yet Another Centralized Scheduler

## Execution Instructions
Terminal 1: python3 master.py path_to_config.json algorithm_type('RR','RANDOM','LL')  

Terminal 2: python3 worker.py port worker_id

The above ^ has to be repeated based on the config file provided. The process can be automated however this assumes all workers would run on the same system and not on different ones. The communication between master and worker is done using localhost however if it is on different systems the IP address must be set appropriately.

Terminal Last: python3 requests.py number_of_requests 

Two log files are created for each algorithm and after the files have been created for **all** algorithms analysis.py can be run to obtain the visualizations.

python3 analysis.py

Modules utilized for the visualizations:
* math
* pandas 
* matplotlib.pyplot 
* numpy 

Modules utilized for Master and Worker:
* os
* sys
* csv
* json
* copy
* socket
* random
* threading
* time 
