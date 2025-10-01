import os
import time
import psutil
import subprocess
import sys
import requests
from pythonping import ping
import speedtest
from datetime import datetime
from zoneinfo import ZoneInfo
import boto3
from boto3.s3.transfer import TransferConfig
from dotenv import load_dotenv
load_dotenv()

# Access to the Cloudflare R2 bucket 
AWS_ACCESS_KEY_ID      = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY  = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_ENDPOINT_URL       = os.getenv("AWS_ENDPOINT_URL")
AWS_REGION             = os.getenv("AWS_REGION")

SALAD_MACHINE_ID =  os.getenv("SALAD_MACHINE_ID") 
g_DLSPEED        = int(os.getenv("DLSPEED", "50")) # Mbps
g_ULSPEED        = int(os.getenv("ULSPEED", "20")) # Mbps
g_RTT            = int(os.getenv("RTT","499"))     # ms

S3_CLIENT = boto3.client(
    "s3",
    endpoint_url=AWS_ENDPOINT_URL,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION 
)


# Test network bandwdith
def network_test():
    #print("Test the network speed ....................", flush=True)
    try:
        speed_test = speedtest.Speedtest()
        bserver    = speed_test.get_best_server()
        dlspeed    = int(speed_test.download() / (1000 * 1000))  # Convert to Mbps, not Mib
        ulspeed    = int(speed_test.upload() / (1000 * 1000))  # Convert to Mbps, not Mib
        latency    = bserver['latency'] # the RTT to the selected test server
        country    = bserver['country'] 
        location   = bserver['name']
    except Exception as e:  
        # Some ISPs may block speed test traffic; in such cases, we fall back to the default network performance for the node.
        return "none", "none", g_RTT, g_DLSPEED, g_ULSPEED
    return country, location, latency, dlspeed, ulspeed


# Test network latency
# Only the root user can run this code - no issue in containers
def ping_test(tCount=10):
    if tCount ==0:
        return g_RTT, g_RTT, g_RTT
    try:
        #print("To: ec2.us-west-1.amazonaws.com")
        temp = ping('ec2.us-west-1.amazonaws.com', interval=1, count=tCount, verbose=False)
        latency_uswest1 = temp.rtt_avg_ms # average of successful pings only     
        #print("To: ec2.us-east-2.amazonaws.com")
        temp = ping('ec2.us-east-2.amazonaws.com', interval=1, count=tCount, verbose=False)
        latency_useast2 = temp.rtt_avg_ms # average of successful pings only     
        #print("To: ec2.eu-central-1.amazonaws.com")  
        temp = ping('ec2.eu-central-1.amazonaws.com', interval=1, count=tCount,verbose=False)
        latency_eucentral1 = temp.rtt_avg_ms # average of successful pings only.
    except Exception as e:  
        return g_RTT, g_RTT, g_RTT
    return latency_uswest1, latency_useast2, latency_eucentral1


# Read the supported CUDA RT Version
def Get_CUDA_Version():
    try:
        cmd = 'nvidia-smi'
        output = subprocess.check_output(cmd, shell=True, text=True)
        output = output.split("\n")[2]
        output = output.split("CUDA Version: ")[-1]
        version = float(output.split(" ")[0])
    except Exception as e: 
        return 0
    return version 


# Get the GPU info
def Get_GPUs():
    try:
        cmd = ('nvidia-smi --query-gpu=gpu_name,memory.total,memory.used,memory.free,'
               'utilization.memory,temperature.gpu,utilization.gpu --format=csv,noheader,nounits')
        output = subprocess.check_output(cmd, shell=True, text=True)
        lines = output.strip().split('\n')
        for line in lines: # 1 and 8 ( few 2 )
            gpu_name, vram_total, vram_used, vram_free, mem_util, temp, gpu_util = line.strip().split(', ')
            result = {
                'gpu_type': gpu_name,
                'gpu_number': len(lines),
                'gpu_vram_total_MiB': int(vram_total),
                'gpu_vram_used_MiB': int(vram_used),
                'gpu_vram_used_percent_%': int((int(vram_used)/int(vram_total))*100), # 0-100
                'gpu_utilization_%': int(gpu_util),
                'gpu_temperature_C': int(temp),
                'gpu_vram_utilization_%': int(mem_util) # VRAM <-> GPU Cache
            }
            break
        return result
    except Exception as e:
        return {}


# Get the CPU info
def Get_CPUs():
    try:
        cpu_percent = psutil.cpu_percent(interval=1, percpu=False) # the percentage of total CPU resources being used.
        cpu_freq = psutil.cpu_freq().current
        num_vcpus = psutil.cpu_count(logical=True)
        virtual_mem = psutil.virtual_memory()
        cpu_type = "Unknown"

        with open("/proc/cpuinfo") as f:
            for line in f:
                if "model name" in line:
                    cpu_type = line.split(":")[1].strip()
                    break

        return { "cpu_type": cpu_type,  
                 "cpu_num_vcpus": num_vcpus,      
                 "cpu_freq_MHz": int(cpu_freq),
                 "cpu_percent_%": cpu_percent, # 0-100
                 "cpu_ram_total_B": virtual_mem.total,
                 "cpu_ram_used_B": virtual_mem.used,
                 "cpu_ram_used_%": virtual_mem.percent # 0-100
                 }
    except Exception as e:
        return {}

def System_Check(NETWORK_TEST=True):    

    online_time = datetime.now(ZoneInfo("UTC")).strftime("%Y-%m-%d %H:%M:%S")

    # CUDA Version
    CUDA_version = Get_CUDA_Version()
    #print("CUDA Version:", CUDA_version)
    
    # GPU Info
    GPUS = Get_GPUs()
    #print("GPU Info:", GPUS)
    
    CPUS =  Get_CPUs()
    #print("CPU Info:", CPUS)

    Pass = True
    if CUDA_version == 0 or GPUS == {} or CPUS == {}: 
        Pass = False

    NETWORK = {}
    Network_Pass = True
    if SALAD_MACHINE_ID != "local" and NETWORK_TEST == True:       # Skip the initial checks if run locally    
        # Network test: bandwidth
        country, location, latency, dlspeed, ulspeed = network_test() 
        #print(f"Networt: {country}, {location}, DL {dlspeed} Mbps, UL {ulspeed} Mbps")
        # Network test: latency to some locations; should reallocate if ping fails
        latency_us_w, latency_us_e, latency_eu = ping_test(tCount = 5) 
        #print(f"Latency: to US West {latency_us_w} ms, to US East {latency_us_e} ms, to EU Central {latency_eu} ms")
        if ulspeed < g_ULSPEED or dlspeed < g_DLSPEED or latency_us_w > g_RTT or latency_us_e > g_RTT or latency_eu > g_RTT:
            Network_Pass = False
 
        NETWORK = { "country":            country,
                    "location":           location,
                    "rtt_ms":             str(latency),
                    "upload_Mbps":        str(ulspeed),
                    "download_Mbps":      str(dlspeed), 
                    "rtt_to_us_west1_ms": str(latency_us_w),                        
                    "rtt_to_us_east2_ms": str(latency_us_e),
                    "rtt_to_eu_cent1_ms": str(latency_eu),
                    "network_pass":       Network_Pass
                 } 
        
    history_column = 'no, timestamp, gpu_vram_used_percent_%, gpu_vram_utilization_%, gpu_utilization_%, gpu_temperature_C, cpu_percent_%, cpu_ram_used_%'
    history_column = history_column + ', performance_sol_s, power_watts, core_temp_C, core_clock_MHz, accepted, rejected'

    environment= { "online":             online_time,           
                   "last_update":        online_time,   
                   "uptime_s":           0,    # placeholder, will be updated later
                   "no":                 0,    # placeholder, will be updated later
                   'metric_interval_s':  0,    # placeholder, will be updated later
                   'report_number':      0,    # placeholder, will be updated later
                   'miner_algorithm':    "",   # placeholder, will be updated later
                   "miner_state":       "running",
                   "salad_machine_id":   SALAD_MACHINE_ID,
                   "pass":               Pass,
                 } | { "gpu_cuda_version": CUDA_version } | GPUS | CPUS | NETWORK | { 'history_column': history_column } | { "history": [] } # Add an empty history list
    
    return environment


# Trigger node reallocation if a node is not suitable
# https://docs.salad.com/products/sce/container-groups/imds/imds-reallocate
def Reallocate(reason):
    local_run = True if 'local' in SALAD_MACHINE_ID.lower() else False
    
    print(reason)

    if (local_run):  # Run locally
        print("Call the exitl to restart ......", flush=True) 
        os.execl(sys.executable, sys.executable, *sys.argv)
    else:            # Run on SaladCloud
        print("Call the IMDS reallocate ......", flush=True)
        url = "http://169.254.169.254/v1/reallocate"
        headers = {'Content-Type': 'application/json',
                   'Metadata': 'true'}
        body = {"Reason": reason}
        _ = requests.post(url, headers=headers, json=body)
        time.sleep(10)


# Upload source to bucket/prefix/folder/target
# Not output any messages to stdout
def Uploader_Chunked_Parallel(
    task,
    source, 
    bucket, prefix, folder, target,
    chunk_size_mbtype, # e.g., "10M"
    concurrency        #  e.g., "10"
):
    s3 = S3_CLIENT 

    # Get the size of source file in MB before uploade
    try:
        fileSize = os.path.getsize(source)
        fileSizeMB = fileSize / 1_000_000
    except Exception as e:
        return {f"{task}_error_filesize": str(e)}
    
    chunk_size_mb = int(''.join(filter(str.isdigit, chunk_size_mbtype)))
    multipart_chunksize = chunk_size_mb * 1_000_000
    max_concurrency = int(concurrency)
    if prefix == "":
        key = f"{folder}/{target}"
    else:
        key = f"{prefix}/{folder}/{target}"

    config = TransferConfig(
        multipart_chunksize=multipart_chunksize,
        max_concurrency=max_concurrency, # ignored if use_threads is False
        use_threads=True
    )

    # Start
    startTime = time.time()
    
    try:
        s3.upload_file(
            Filename=source,
            Bucket=bucket,
            Key=key,
            Config=config
        )
    except Exception as e:
        return { f"{task}_error_upload": str(e) }
    
    # End
    timeSec = time.time() - startTime
    throughputMbps = (fileSizeMB * 8) / timeSec 

    return {
        "uploaded_file": target,
        f"{task}_size_MB": f"{fileSizeMB:.3f}",
        f"{task}_time_second": f"{timeSec:.3f}",
        f"{task}_throughput_Mbps": f"{throughputMbps:.3f}"
    }