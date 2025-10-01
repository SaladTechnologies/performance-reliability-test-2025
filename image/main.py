import time
import os
import shutil
import threading
import subprocess
import queue
import uuid
import json
import requests
from dotenv import load_dotenv
from helper import System_Check, Uploader_Chunked_Parallel, Reallocate

load_dotenv()

# Wallet
WALLET = os.environ.get("WALLET")

# The target bucket and prefix/folder
BUCKET     = os.getenv("BUCKET")
PREFIX     = os.getenv("PREFIX","")  # Can be empty 
FOLDER     = os.getenv("FOLDER")

METRIC_INTERVAL = int(os.getenv("METRIC_INTERVAL", 60)) # 60 seconds
REPORT_NUMBER   = int(os.getenv("REPORT_NUMBER",    5))  # 5, report to cloud every 5 * 60 = 300 seconds

MAX_NO_RESPONSE_TIME     = METRIC_INTERVAL * REPORT_NUMBER * 2  # 600 seconds, no report for 10 minutes, then reallocate
MAX_UPLOAD_FAILURE_COUNT = 2   # 2 consecutive report failures, then reallocate
UPLOAD_CHECK_INTERVAL    = 5   # 5 seconds, check the upload queue every 5 seconds

LOCAL_LOG_FILE = "miner_run.log"
API_URL = "http://localhost:8080/"
# automatically optimizes LHR cards. Non-LHR GPUs ignore it safely.
CMD_ZELHASH = [ "./lolMiner", "-a", "FLUX",    "--pool", "stratum+tcp://zelhash.auto.nicehash.com:9200","--user", WALLET, "--apiport", "8080","--lhrtune", "auto" ]
#CMD_OCTOPUS = [ "./lolMiner", "-a", "OCTOPUS", "--pool", "stratum+tcp://octopus.auto.nicehash.com:9200","--user", WALLET, "--apiport", "8080" ]

NO = 0 # of metric collections
RESULT = {}
START = time.perf_counter()
GLOBAL_LOCK = threading.Lock()

def get_mining_performance():
    try:

        # Can implement the real-time performance minitoring here:
        # if performance_sol_s is zero or decreased in a specific time window, call the IMDS reallocate
        response = requests.get(API_URL, timeout=1)
        data = response.json()

        algo       = data["Algorithms"][0]
        total_perf = algo["Total_Performance"]
        unit       = algo["Performance_Unit"]
        accepted   = algo["Total_Accepted"]
        rejected   = algo["Total_Rejected"]

        worker     = data["Workers"][0]
        power      = worker["Power"]
        core_temp  = worker["Core_Temp"]
        cclk       = worker["CCLK"]
        
        return { "performance_sol_s": total_perf,
                 "power_watts": power,
                 "core_temp_C": core_temp,
                 "core_clock_MHz": cclk,
                 "accepted": accepted,
                 "rejected": rejected }
    except Exception as e:
        return {"error": str(e)}
    
# Collect system metrics and put the report job into the queue
def Metric_Task(queue):
    global RESULT, NO

    # The 1st may take over 1 minutes, due to the network test
    with GLOBAL_LOCK:  # Ensure only one thread at a time can update RESULT
        END = time.perf_counter()
        if RESULT == {}: # First time only
            RESULT = temp = System_Check(NETWORK_TEST=True) 
            RESULT['metric_interval_s'] = METRIC_INTERVAL
            RESULT['report_number']     = REPORT_NUMBER
            RESULT['miner_algorithm']   = "zelhash" # "octopus" if RESULT['gpu_vram_total_MiB'] >= 12000 else "zelhash"
        else:            # Skip the network test for subsequent runs
            temp = System_Check(NETWORK_TEST=False)
    
        RESULT['last_update'] = temp['last_update']
        RESULT['uptime_s']    = round(END - START,3)
        RESULT['no']          = NO

        if temp["pass"] == True: # Successfully collected CUDA/GPU/CPU metrics
            value = f"{NO},{temp['last_update']},{temp['gpu_vram_used_percent_%']},{temp['gpu_vram_utilization_%']},{temp['gpu_utilization_%']},{temp['gpu_temperature_C']},{temp['cpu_percent_%']},{temp['cpu_ram_used_%']}"
        else:
            value = f"{NO},{temp['last_update']},0,0,0,0,0,0"

        temp_value = get_mining_performance()   
        if len(temp_value) > 1: # Successfully collected mining metrics
            # print(f'\n+++++++++> Metric Task Thread: get mining performance - {temp_value}', flush=True)
            value = value + f",{round(temp_value['performance_sol_s'],3)},{temp_value['power_watts']},{temp_value['core_temp_C']},{temp_value['core_clock_MHz']},{temp_value['accepted']},{temp_value['rejected']}"
        else:
            print(f'\n+++++++++> Metric Task Thread: errors while getting mining performance: {temp_value}', flush=True)
            value = value + ",0,0,0,0,0,0"

        RESULT['history'].append(value)
    
        if NO % REPORT_NUMBER == 0: 
            UID = str(uuid.uuid4()) + ".txt"
            FILE_NAME = RESULT["online"].replace(":", "-").replace(" ", "_") + "_" + temp["salad_machine_id"] + ".txt"
            with open(UID, 'w') as f:
                json.dump(RESULT, f, indent=2)
            queue.put( {'source': UID, 'filename': FILE_NAME, 'no': str(NO)} )  

        print(f'\n+++++++++> Metric Task Thread: collected metrics - {value}', flush=True)

        NO += 1


def Scheduler(queue, interval, func):     
    def loop():                    # Run and then sleep
        next_time = time.time()    # Initial start time
        while True:                
            next_time += interval  # Next start time
            threading.Thread(target=func, args=(queue,)).start()  # Run task in its own thread (non-blocking)
            time.sleep( max(0, next_time - time.time()) )         # Sleep by removing the potential drift

    threading.Thread(target=loop, daemon=True).start()    # Start the scheduler thread in the background


# Read jobs from the queue, which refer to local temp files to be uploaded
# After upload, keep the local copy and delete the temp file
def Uploader(queue):
    def loop():
        UPLOAD_FAILURE_COUNT = 0 # I/O Failures
        NO_RESPONSE_TIME = 0     # Metric Task Failures
        while True:
            if queue.empty():
                time.sleep(UPLOAD_CHECK_INTERVAL)
                NO_RESPONSE_TIME += UPLOAD_CHECK_INTERVAL
                if NO_RESPONSE_TIME >= MAX_NO_RESPONSE_TIME:
                    Reallocate("Metric Task Failures")
                continue

            NO_RESPONSE_TIME = 0 # Reset the counter after getting a job successfully

            message = queue.get()  # May block here
            queue.task_done()

            result = Uploader_Chunked_Parallel(message['no'], message['source'],BUCKET, PREFIX, FOLDER, message['filename'], '1MB',10)
            print(f"\n---------> Uploader Thread: report metrics to {message['no']} - {result}", flush=True)

            if len(result) <= 1:  # Upload failed
                UPLOAD_FAILURE_COUNT += 1
                if UPLOAD_FAILURE_COUNT >= MAX_UPLOAD_FAILURE_COUNT:
                    Reallocate("2 or more consecutive upload failures")
            else:                 # Upload succeeded
                UPLOAD_FAILURE_COUNT = 0  # Reset the counter after uploading a file successfully

            shutil.copy(message['source'], message['filename']) # Update the local copy using the temp file
            os.remove(message['source'])                        # Remove the temp file

    threading.Thread(target=loop, daemon=True).start()    # Start the uploader thread in the background


# For communication between the metric task and the uploader
upload_queue = queue.Queue() 

print("\nStarting the scheduler thread to collect metrics ...")
Scheduler(upload_queue, METRIC_INTERVAL, Metric_Task)

print("\nStarting the uploader thread ...")
Uploader(upload_queue)

print("\nThe miner: wait until the first metrics are collected ...")
while True:
    if RESULT == {}: 
        time.sleep(2)
    else:
        time.sleep(2)
        break

cmd = CMD_ZELHASH # CMD_OCTOPUS if RESULT['gpu_vram_total_MiB'] >= 12000 else CMD_ZELHASH
print("\nStarting the Miner: " + " ".join(cmd))

#subprocess.run(cmd) # Restart/Reallocate/Reallocate here after the miner stops for any reason

with open(LOCAL_LOG_FILE, "w") as f:
    # Run the miner and redirect stdout/stderr to the file
    subprocess.run(cmd, stdout=f, stderr=f) # Restart/Reallocate/Reallocate here after the miner stops for any reason

RESULT["miner_state"] = "stopped"
time.sleep(1000000) # 11+ days