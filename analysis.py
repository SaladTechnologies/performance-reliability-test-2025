import os
import json
from datetime import datetime, timedelta
from collections import Counter


FOLDER_PATH = "data"

TIMESTAMP_START       = datetime.strptime('2025-09-22 00:00:00', '%Y-%m-%d %H:%M:%S')
TIMESTAMP_2HOUR       = datetime.strptime('2025-09-22 02:00:00', '%Y-%m-%d %H:%M:%S')
TIMESTAMP_3HOUR       = datetime.strptime('2025-09-22 03:00:00', '%Y-%m-%d %H:%M:%S')
TIMESTAMP_1DAY        = datetime.strptime('2025-09-23 00:00:00', '%Y-%m-%d %H:%M:%S')
TIME_INTERVAL_1MIN    = timedelta(minutes=1)
TIME_INTERVAL_5MIN    = timedelta(minutes=5)
TIME_INTERVAL_10MIN   = timedelta(minutes=10)
TIME_INTERVAL_1HOUR   = timedelta(minutes=60)
TIME_INTERVAL_1DAY    = timedelta(minutes=1440)


def Get_DataList(folder_path):
    data_list = []

    # List all files (excluding directories)
    files = [ f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f)) ]
    print(f"----> Number of files in the folder: {len(files)}")

    for filename in os.listdir(folder_path):
        if filename.endswith(".txt"):  # process .txt files
            file_path = os.path.join(folder_path, filename)
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()          # read the file as a string
                data = json.loads(content)  # parse JSON from string
                data_list.append(data)
    data_list.sort(key=lambda x: x['online'])
    print(f"----> Number of Node Runs: {len(data_list)}")

    return data_list


def Get_TimestampRange(data_list):
    if not data_list:
        return None, None
    start = min(datetime.strptime(item['online'], '%Y-%m-%d %H:%M:%S') for item in data_list)
    end = max(datetime.strptime(item['last_update'], '%Y-%m-%d %H:%M:%S') for item in data_list)
    return start, end


def Get_AbnormalNodeRuns(data_list):
    abnormal_node_runs = []

    # Inconsistent Data
    for node_run in data_list:

        value11 = node_run['no']
        value12 = int(node_run['history'][-1].split(",")[0])
        value21 = node_run['last_update']
        value22 = node_run['history'][-1].split(",")[1]
        if value11 != value12 or value21 != value22:
            abnormal_node_runs.append( ('Inconsistent Data', node_run['online']) )
            continue

        # The lolMiner failures
        if node_run['miner_state'] != "running":
            abnormal_node_runs.append( ('lolMiner Failures', node_run['online']) )
            continue

        # No mining activity
        temp = int(node_run['history'][-1].split(",")[-2])
        if temp == 0:
            abnormal_node_runs.append( ('No Mining Activity', node_run['online'] ))
            continue

        # Mining stopped for 10 minutes
        temp = int(node_run['history'][-1].split(",")[-2])
        if len(node_run['history']) >10:
            temp10 = int(node_run['history'][-10].split(",")[-2])
            if temp == temp10:
                abnormal_node_runs.append( ('Mining Stopped', node_run['online'] ))

    return abnormal_node_runs


def Get_ActiveInstanceNumber(data_list, start, end, interval):
    active_instance_number = []
    current_time = start

    while True:
        if current_time >= end:
            break
        count = 0
        for node_run in data_list:
            nStart = datetime.strptime(node_run['online'], '%Y-%m-%d %H:%M:%S')
            nEnd = datetime.strptime(node_run['last_update'], '%Y-%m-%d %H:%M:%S')
            if nStart <= current_time <= nEnd:
                count += 1
        active_instance_number.append( (current_time.strftime('%Y-%m-%d %H:%M:%S'), count) )
        #if count > 100:
        #    print (f"Warning: More than 100 active instances at {current_time.strftime('%Y-%m-%d %H:%M:%S')}: {count}")
        current_time += interval

    # Extract just the counts
    counts = [c for _, c in active_instance_number]

    if counts:
        avg_count = sum(counts) / len(counts)
        min_count = min(counts)
        max_count = max(counts)
        print(f"Average active instances: {avg_count:.2f}")
        print(f"Minimum active instances: {min_count}")
        print(f"Maximum active instances: {max_count}")
    else:
        print("No active instances found in the given interval.")
        
    return active_instance_number


# For all nodes, including stopped and running nodes
def Get_Allocation(data_list, start, end, interval):
    node_allocation = []
    current_time = start
    
    while True:
        if current_time >= end:
            break
        count = 0
        for node_run in data_list:
            nStart = datetime.strptime(node_run['online'], '%Y-%m-%d %H:%M:%S')
            if current_time <= nStart < current_time + interval:
                count += 1
        node_allocation.append( (current_time.strftime('%Y-%m-%d %H:%M:%S'), count) )
        current_time += interval

    # Extract just the counts
    counts = [c for _, c in node_allocation]

    if counts:
        avg_count = sum(counts) / len(counts)
        min_count = min(counts)
        max_count = max(counts)
        print(f"Average allocation number: {avg_count:.2f}")
        print(f"Minimum allocation number: {min_count}")
        print(f"Maximum allocation number: {max_count}")
    else:
        print("No allocation found in the given interval.")

    return node_allocation


# For a node, we cannot determine its real uptime until it has stopped.
# if last_update is a few minutes earlier than end, it means the node_run has stopped.
def Get_Uptimes(data_list, start, end, mode, gpu):
    node_uptimes = []

    for node_run in data_list:
        online     = datetime.strptime(node_run['online'], '%Y-%m-%d %H:%M:%S')
        last_update = datetime.strptime(node_run['last_update'], '%Y-%m-%d %H:%M:%S')

        if online < start or online >= end:
            continue

        if gpu == "high":
            if node_run["gpu_vram_total_MiB"] < 20000:
                continue
        elif gpu == "low":
            if node_run["gpu_vram_total_MiB"] >= 10000:
                continue
        else: # "all":
            pass

        if mode == "stopped":
            if last_update < end - TIME_INTERVAL_10MIN:
                node_uptimes.append( (node_run['uptime_s'], node_run['online'], node_run['last_update']) ) 
        elif mode == "running":
            if end - TIME_INTERVAL_10MIN <= last_update:  
                node_uptimes.append( (node_run['uptime_s'], node_run['online'], node_run['last_update']) ) 
        else:     #  "all"
            node_uptimes.append( (node_run['uptime_s'], node_run['online'], node_run['last_update']) ) 
    
    # print(len(node_uptimes))

    total_nodes = len(node_uptimes)
    less_than_1h_count = sum(1 for u, _, _ in node_uptimes if u < 3600)  # <1 hour
    less_than_1h_pct = (less_than_1h_count / total_nodes * 100) if total_nodes > 0 else 0

    print(f"Total nodes: {total_nodes}")
    print(f"Nodes with <1 hour uptime: {less_than_1h_count} ({less_than_1h_pct:.1f}%)")

    return node_uptimes


# Get average, min, max uptimes from a list of uptimes
def Get_Uptimes_Ave_Min_Max(node_uptimes):
    uptimes = [uptime[0] for uptime in node_uptimes]
    average_uptime = sum(uptimes) / len(uptimes)
    min_uptime = min(uptimes)
    max_uptime = max(uptimes)
    return average_uptime, min_uptime, max_uptime

def Get_Top10_GPU_Types(data_list):
    gpu_types = []
    print(f"----> Number of Node Runs: {len(data_list)}")
    # collect all gpu types
    for node_run in data_list:
        gpu_type = node_run.get('gpu_type')
        if gpu_type:
            gpu_types.append(gpu_type)

    # count them
    counts = Counter(gpu_types)

    # take top 10
    top_10 = counts.most_common(10)

    # group the rest into "others"
    others_count = sum(count for gpu, count in counts.items() if (gpu, count) not in top_10)
    if others_count > 0:
        top_10.append(("others", others_count))

    # print nicely
    print("GPU Types and Counts:")
    total = len(gpu_types)  # only consider entries that had gpu_type
    for gpu, count in top_10:
        print(f"{gpu}: {count}, {count/total*100:.1f}%")

    return top_10

def Get_GPU_Types(data_list):
    gpu_types = []

    # collect all gpu types
    for node_run in data_list:
        gpu_type = node_run.get('gpu_type')
        if gpu_type:
            gpu_types.append(gpu_type)

    # count them
    counts = Counter(gpu_types)

    # sort by count (descending)
    sorted_counts = sorted(counts.items(), key=lambda x: x[1], reverse=True)

    # print nicely
    print("GPU Types and Counts:")
    for gpu, count in sorted_counts:
        print(f"{gpu}: {count}, {count/len(data_list)*100:.1f}%")

    return sorted_counts



def Get_Top10_Countries(data_list):
    countries = []
    print(f"----> Number of Node Runs: {len(data_list)}")
    # collect all countries
    for node_run in data_list:
        country = node_run.get('country')
        if country:
            countries.append(country)

    # count them
    counts = Counter(countries)

    # take top 10
    top_10 = counts.most_common(10)

    # group the rest into "others"
    others_count = sum(count for c, count in counts.items() if (c, count) not in top_10)
    if others_count > 0:
        top_10.append(("others", others_count))

    # print nicely
    print("Country Types and Counts:")
    total = len(countries)  # only consider entries that had country
    for country, count in top_10:
        print(f"{country}: {count}, {count/total*100:.1f}%")

    return top_10

def Get_Countries(data_list):
    countries = []

    # collect all countries
    for node_run in data_list:
        country = node_run.get('country')
        if country:
            countries.append(country)

    # count them
    counts = Counter(countries)
    # sort by count (descending)
    sorted_counts = sorted(counts.items(), key=lambda x: x[1], reverse=True)

    # print nicely
    print("Country Types and Counts:")
    for country, count in sorted_counts:
        print(f"{country}: {count}, {count/len(data_list)*100:.1f}%")

    return sorted_counts

def Get_Performance_Variance(data_list, gpu_type):
    performance_data = []

    for node_run in data_list:
        if node_run['gpu_type'] == gpu_type:
            if len(node_run['history']) < 10:
                continue
            value = 0 # sum of the last 10 performance values
            for entry in node_run['history'][-10:]:
                value += float(entry.split(",")[-6])
            temp = round(value/10,3)
            performance_data.append( (temp, node_run['online']) )
 
    if not performance_data:
        print(f"No performance data found for GPU type: {gpu_type}")

    return performance_data


DATA_LIST = Get_DataList(FOLDER_PATH)

if __name__ == "__main__":
    
    
    start, end = Get_TimestampRange(DATA_LIST)
    print(f"----> The 1st  online: {start}")
    print(f"----> The last update: {end}")

    abnormal_node_runs = Get_AbnormalNodeRuns(DATA_LIST)
    print(f"----> Number of Abnormal Node Runs: {len(abnormal_node_runs)}")
    
    print("----> Abnormal Node Runs:")
    for bad in abnormal_node_runs:
        print(bad)

    print("----> GPU Types and Counts:")
    Get_Top10_GPU_Types(DATA_LIST)

    print("----> Country Types and Counts:")
    Get_Top10_Countries(DATA_LIST)


