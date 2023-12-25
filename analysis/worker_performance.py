import json
from matplotlib import pyplot as plt


def plot_filtered_data(data, title):
    filtered_data = [item for item in data if item['workerCount'] in [2, 3, 4, 5]]
    worker_counts = [item['workerCount'] for item in filtered_data]
    execution_times = [item['executionTime'] for item in filtered_data]
    
    plt.figure(figsize=(10, 6))
    plt.scatter(worker_counts, execution_times, label=title)
    plt.plot(worker_counts, execution_times)
    plt.xlabel('Worker Count')
    plt.ylabel('Execution Time (ms)')
    plt.title(title)
    plt.legend()
    plt.grid(True)
    plt.xticks([2, 3, 4, 5]) 
    plt.savefig(title)
    plt.show()

def load_data(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

data_q3 = load_data("../results/Q3_DistributionOfSchedClass_execution_times.json")
data_q4 = load_data("../results/Q4_ClassEvictionProbability_execution_times.json")

plot_filtered_data(data_q3, 'Distribution of Sched Class Execution Times')

plot_filtered_data(data_q4, 'Class Eviction Probability Execution Times')

