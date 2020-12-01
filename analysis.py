# We expect all three algorithms to be run before plotting
import math
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Reading task logs
tasks_ll = pd.read_csv("task_logLL.csv")
tasks_rr = pd.read_csv("task_logRR.csv")
tasks_random = pd.read_csv("task_logRANDOM.csv")

# Reading job logs
jobs_ll = pd.read_csv("job_logLL.csv")
jobs_rr = pd.read_csv("job_logRR.csv")
jobs_random = pd.read_csv("job_logRANDOM.csv")

# Function to generate heatmaps for a particular algorithm
def genHeatMap(tasks,title):
    max_time = tasks['Completion'].max()
    min_time = tasks['Completion'].min()
    duration = math.ceil(max_time - min_time)
    worker_count = len(tasks['WorkerId'].unique())

    matrix = [[0 for time in range(duration + 2)] for i in range(worker_count)]

    worker_ids = list(tasks['WorkerId'].unique())
    index = 0
    for i in worker_ids:
        df = tasks[tasks['WorkerId'] == i]
        for num in range(duration):
            curr_time = min_time + num
            count = len(df[(df["Completion"] > curr_time) & (df["ArrivalTime"] < curr_time)])
            matrix[index][num + 1] = count
        index += 1

    fig, ax = plt.subplots(figsize =(10, 7))
    im = ax.imshow(matrix,cmap='YlOrRd_r')

    x_axis = list(range(duration + 2))
    workers = ['worker_' + str(i) for i in worker_ids]
    ax.set_xticks(np.arange(len(x_axis)))
    ax.set_yticks(np.arange(len(workers)))
    ax.set_xticklabels(x_axis)
    ax.set_yticklabels(workers)

    plt.setp(ax.get_xticklabels(), rotation=45, ha="right",
            rotation_mode="anchor")

    for i in range(len(workers)):
        for j in range(len(x_axis)):
            text = ax.text(j, i, matrix[i][j],
                        ha="center", va="center", color="black")

    ax.set_title(title)
    fig.tight_layout()
    plt.show()

# Function to generate median and mode graphs for tasks
def genTaskGraph():
    
    tasks_rr_mean = tasks_rr['ExecutionTime'].mean()
    tasks_rr_median = tasks_rr['ExecutionTime'].median()

    tasks_random_mean = tasks_random['ExecutionTime'].mean()
    tasks_random_median = tasks_random['ExecutionTime'].median()
    
    tasks_ll_mean = tasks_ll['ExecutionTime'].mean()
    tasks_ll_median = tasks_ll['ExecutionTime'].median()

    means = [tasks_rr_mean,tasks_random_mean,tasks_ll_mean]
    plt.figure(figsize =(10, 7)) 
    plt.barh(['Round Robin','Random','Least Loaded'],means)
    plt.title("Task Mean Execution Times") 
    for index, value in enumerate(means):
        plt.text(value, index, str(round(value,2)))
    plt.show()

    medians = [tasks_rr_median,tasks_random_median,tasks_ll_median]
    plt.figure(figsize =(10, 7)) 
    plt.barh(['Round Robin','Random','Least Loaded'],medians) 
    plt.title("Task Median Execution Times")
    for index, value in enumerate(medians):
        plt.text(value, index, str(round(value,2)))
    plt.show()

# Function to generate median and mode graphs for jobs
def genJobGraph():
    jobs_rr_mean = jobs_rr['ExecutionTime'].mean()
    jobs_rr_median = jobs_rr['ExecutionTime'].median()

    jobs_random_mean = jobs_random['ExecutionTime'].mean()
    jobs_random_median = jobs_random['ExecutionTime'].median()
    
    jobs_ll_mean = jobs_ll['ExecutionTime'].mean()
    jobs_ll_median = jobs_ll['ExecutionTime'].median()

    means = [jobs_rr_mean,jobs_random_mean,jobs_ll_mean]
    plt.figure(figsize =(10, 7)) 
    plt.barh(['Round Robin','Random','Least Loaded'],means)
    plt.title("Jobs Mean Execution Times") 
    for index, value in enumerate(means):
        plt.text(value, index, str(round(value,2)))
    plt.show()

    medians = [jobs_rr_median,jobs_random_median,jobs_ll_median]
    plt.figure(figsize =(10, 7)) 
    plt.barh(['Round Robin','Random','Least Loaded'],medians) 
    plt.title("Jobs Median Execution Times")
    for index, value in enumerate(medians):
        plt.text(value, index, str(round(value,2)))
    plt.show()

genTaskGraph()
genJobGraph()
genHeatMap(tasks_ll,"Least Loaded")
genHeatMap(tasks_rr,"Round Robin")
genHeatMap(tasks_random,"Random")