import json
from matplotlib import pyplot as plt


def plot_consumption(resourceName,input):
    values = input['values']
    averages =  input['avg'] 
    std_devs = input['std']

    #values = [x * 1000 for x in test_list]
    # Plotting
    plt.figure(figsize=(8, 6))
    # TO plot with standard dev
    plt.errorbar(values, averages, yerr=std_devs, fmt='o', capsize=2, markersize=3)
    #plt.errorbar(values, averages, fmt='o', capsize=2, markersize=1)
    plt.plot(values, values, linestyle='--', color='red')  # Plot y = x line
    plt.xlabel(f'Requested {resourceName}')
    plt.ylabel(f'Used {resourceName}')
    plt.title(f'Variation of {resourceName} used compared to {resourceName} requested')
    plt.grid(True)
    plt.show()

def plot_resource_consumption(json_file):
    data_cpu = json_file['Q6_UsagePerRequest']['cpu']
    data_mem = json_file['Q6_UsagePerRequest']['mem']
    data_disk = json_file['Q6_UsagePerRequest']['disk']

    plot_consumption('cpu',data_cpu)
    plot_consumption('memory',data_mem)
    plot_consumption('disk',data_disk)


# ------------
def plot_corr(resourceName,input):


    


    resource_percent_usage = input['resource_percent_usage']
    cumulative_freq =  input['cumulative_freq'] 

    cnt = 0
    for i in resource_percent_usage:
        if i <=1:
            cnt=cnt+1
    resource_percent_usage = resource_percent_usage[0:cnt]
    cumulative_freq = cumulative_freq[0:cnt]

    # Plotting the increasing cumulative frequency graph
    plt.plot(resource_percent_usage, cumulative_freq, marker='o',markersize=1, label='Cumulative Evictions')
    
    # plt.plot([point[0] for point in sorted_data], [point[1] for point in sorted_data], marker='o', markersize=1)
    plt.xlabel(f'{resourceName} usage percentage')
    plt.ylabel('Cumulative evictions')
    plt.title('Increasing Cumulative Evictions Plot')
    plt.grid(True)
    plt.legend()
    plt.show()



def plot_correlation(json_file):
    data_cpu = json_file['Q7_CorrelationPeakAndEvictions']['cpu']
    data_mem = json_file['Q7_CorrelationPeakAndEvictions']['mem']

    plot_corr('cpu',data_cpu)
    plot_corr('memory',data_mem)


# -------------
    
def plot_evict_prob(json_file):
    data = json_file['Q4_class_eviction_probabiltiy']

    x = [str(pr[0]) for pr in data]
    y = [pr[1] for pr in data]

    plt.figure(figsize=(10,6))
    plt.bar(x,y,color='lightblue',edgecolor='black') 
    plt.xlabel(f'Scheduling Class')
    plt.ylabel(f'Eviction Probability')
    plt.grid(True)
    plt.title(f'Eviction Probability By Scheduling Class')
    plt.show()


# Read the JSON file
with open('../results/results.json', 'r') as file:
    json_file = json.load(file)

plot_evict_prob(json_file)
#plot_correlation(json_file)
#plot_resource_consumption(json_file)


# Accessing elements in the JSON data
#print("data_cpu:",data_cpu)