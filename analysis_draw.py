import os
import json
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import MultipleLocator

from analysis import Get_AbnormalNodeRuns, \
    Get_ActiveInstanceNumber, \
    Get_Allocation, \
    Get_Uptimes, Get_Uptimes_Ave_Min_Max, \
    Get_Performance_Variance, \
    DATA_LIST, \
    TIMESTAMP_START,  \
    TIME_INTERVAL_1MIN, TIME_INTERVAL_1HOUR, TIME_INTERVAL_1DAY 


def Plot_Startup_Times(end_time, file_name):
    temp = Get_ActiveInstanceNumber(DATA_LIST, TIMESTAMP_START, end_time, TIME_INTERVAL_1MIN)

    timestamps = [datetime.strptime(t, '%Y-%m-%d %H:%M:%S') for t, _ in temp]
    values = [v for _, v in temp]

    # Convert timestamps -> elapsed minutes from timestamp_start
    elapsed_minutes = [(t - TIMESTAMP_START).total_seconds() / 60 for t in timestamps]

    plt.figure(figsize=(15, 6))
    plt.plot(elapsed_minutes, values, marker='o', markersize=3)
    plt.xlabel('Elapsed Time (minutes since start)')
    plt.ylabel('Running Instances')
    plt.title('Startup Times for 100 Replicas in All Regions — Image Size: 5.53 GB')
    plt.grid(True)

    plt.tight_layout()
    plt.savefig(file_name, dpi=600)


def Plot_Node_Run_to_Request_Ratio(start_time, end_time, title, file_name):
    temp = Get_ActiveInstanceNumber(DATA_LIST, start_time, end_time, TIME_INTERVAL_1MIN)

    timestamps = [datetime.strptime(t, '%Y-%m-%d %H:%M:%S') for t, _ in temp]
    values = [v for _, v in temp]

    plt.figure(figsize=(15, 6))
    plt.plot(timestamps, values, marker='o', markersize=1)
    plt.xlabel('Time')
    plt.ylabel('Running Instances')
    plt.title(title)
    plt.grid(True)

    # Format x-axis to show both date and time
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%y-%m-%d\n%H:%M'))
    plt.gcf().autofmt_xdate()  # Rotate labels to avoid overlap

    plt.tight_layout()
    plt.savefig(file_name, dpi=600)


def Plot_Allocation(start_time, end_time, interval, title, file_name):
    temp = Get_Allocation(DATA_LIST, start_time, end_time, interval)

    timestamps = [datetime.strptime(t, '%Y-%m-%d %H:%M:%S') for t, _ in temp]
    values = [v for _, v in temp]

    plt.figure(figsize=(15, 5))
    plt.plot(timestamps, values, marker='o', markersize=1)
    plt.xlabel('Time')
    plt.ylabel('Instance Allocations')
    plt.title(title)
    plt.grid(True)

    # Format x-axis to show both date and time
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%y-%m-%d\n%H:%M'))
    plt.gcf().autofmt_xdate()  # Rotate labels to avoid overlap

    plt.tight_layout()
    plt.savefig(file_name, dpi=600)


def Plot_Uptime_Distribution(start_time, end_time, mode, gpu, title, file_name): 
    node_uptimes = Get_Uptimes(DATA_LIST, start_time, end_time, mode, gpu)
    uptimes_hr = [uptime[0] / 3600 for uptime in node_uptimes]  # convert seconds to hours

    print(f"Node Runs: {len(uptimes_hr)}")
    ut_ave, ut_min, ut_max = Get_Uptimes_Ave_Min_Max(node_uptimes)
    print(f"Uptime (h) - Ave: {ut_ave/3600:.2f}, Min: {ut_min/3600:.2f}, Max: {ut_max/3600:.2f}")

    # Histogram
    plt.figure(figsize=(15, 5))
    plt.hist(uptimes_hr, bins=40, color='skyblue', edgecolor='black', alpha=0.7)
    plt.xlabel('Uptime (hours)')
    plt.ylabel('Instance Runs')
    plt.title(title + f' ({len(uptimes_hr)} samples)')
    plt.grid(axis='y', alpha=0.75)
    
    # Optionally mark average, min, max (converted to hours)
    plt.axvline(ut_ave/3600, color='red', linestyle='dashed', linewidth=1, 
                label=f'Average: {ut_ave/3600:.2f}h')
    plt.axvline(ut_min/3600, color='green', linestyle='dashed', linewidth=1, 
                label=f'Min: {ut_min/3600:.2f}h')
    plt.axvline(ut_max/3600, color='orange', linestyle='dashed', linewidth=1, 
                label=f'Max: {ut_max/3600:.2f}h')
    plt.legend()

    plt.tight_layout()
    plt.savefig(file_name, dpi=600)
    plt.show()


def Plot_Performance_Single(history, file_prefix, output_dir, messages):
    # --- Storage ---
    timestamps = []
    performance_list = []
    power_list = []
    core_temp_list = []
    core_clock_list = []
    gpu_vram_used_list = []
    gpu_util_list = []
    cpu_ram_used_list = []
    cpu_percent_list = []

    # --- Parse history ---
    for record in history:
        (
            no, timestamp, gpu_vram_used_percent, gpu_vram_utilization,
            gpu_utilization, gpu_temperature_C, cpu_percent, cpu_ram_used,
            performance_sol_s, power_watts, core_temp_C, core_clock_MHz,
            accepted, rejected
        ) = record.split(',')

        try:
            ts = datetime.fromisoformat(timestamp.strip())
        except ValueError:
            ts = timestamp.strip()  # fallback

        timestamps.append(ts)
        performance_list.append(float(performance_sol_s))
        power_list.append(float(power_watts))
        core_temp_list.append(float(core_temp_C))
        core_clock_list.append(float(core_clock_MHz))
        gpu_vram_used_list.append(float(gpu_vram_used_percent))
        gpu_util_list.append(float(gpu_utilization))
        cpu_ram_used_list.append(float(cpu_ram_used))
        cpu_percent_list.append(float(cpu_percent))

    # --- Combined figure with 2 subplots ---
    fig, axs = plt.subplots(2, 1, figsize=(15, 10), sharex=True)

    # --- Subplot 1: Power & Core Clock ---
    axs[0].plot(timestamps, power_list, label="Power (W)", color="green")
    axs[0].plot(timestamps, core_clock_list, label="Core Clock (MHz)", color="blue")
    axs[0].set_ylabel("Value")
    axs[0].set_title(f"Power & Core Clock vs Time ({file_prefix})")
    axs[0].legend(loc="upper left", bbox_to_anchor=(1, 1))
    axs[0].grid(True, alpha=0.3)

    # --- Subplot 2: Performance & Utilization & Core Temp ---
    axs[1].plot(timestamps, performance_list, label="Performance (sol/s)", color="black")
    axs[1].plot(timestamps, gpu_vram_used_list, label="GPU VRAM Used (%)", color="orange")
    axs[1].plot(timestamps, gpu_util_list, label="GPU Utilization (%)", color="brown")
    axs[1].plot(timestamps, cpu_ram_used_list, label="CPU RAM Used (%)", color="pink")
    axs[1].plot(timestamps, cpu_percent_list, label="CPU Utilization (%)", color="gray")
    axs[1].plot(timestamps, core_temp_list, label="Core Temp (°C)", color="red")
    axs[1].set_xlabel("Timestamp")
    axs[1].set_ylabel("Value")
    axs[1].set_title(f"Mining Performance & Utilization Metrics vs Time ({file_prefix})")
    axs[1].legend(loc="upper left", bbox_to_anchor=(1, 1))
    axs[1].grid(True, alpha=0.3)

    # --- Add messages dict as annotation in figure ---
    if messages:
        text_lines = [f"{k}: {v}" for k, v in messages.items()]
        text_str = "\n".join(text_lines)
        fig.text(
            0.99, 0.01, text_str,
            ha="right", va="bottom", fontsize=8,
            bbox=dict(facecolor="white", alpha=0.7, edgecolor="gray")
        )

    # --- Finalize & save ---
    plt.tight_layout(rect=[0, 0.03, 1, 0.97])  # leave space for messages box
    plt.savefig(f"{output_dir}/{file_prefix}.png", dpi=600, bbox_inches="tight")
    plt.show()


def Plot_Abnormal_Samples():
    abnormal_node_runs = Get_AbnormalNodeRuns(DATA_LIST)

    for abnormal in abnormal_node_runs:
        # abnormal[0], reason; abnormal[1], online
        time   = abnormal[1].replace(":", "_").replace(" ", "_").replace("-", "_")
        reason = abnormal[0].replace(" ", "_")

        for node_run in DATA_LIST:
            if node_run['online'] == abnormal[1]:
                machine_id = node_run['salad_machine_id']
                file_prefix = f"{time}_{reason}_{machine_id}"

                messages = {
                    'Uptime_H':    round(node_run['uptime_s']/3600, 2),
                    "CUDA":        node_run['gpu_cuda_version'],
                    "GPU":         node_run['gpu_type'].replace("NVIDIA", "").replace("GeForce", ""),
                    "Location":    node_run['location'],
                    "Country":     node_run['country'],
                }

                Plot_Performance_Single( node_run['history'], file_prefix, "output_abnormal", messages )


def Plot_Normal_Samples(N):
    sorted_list = sorted(DATA_LIST, key=lambda x: x['uptime_s'], reverse=True)
    sorted_list = sorted_list[:N]

    for node_run in sorted_list:

        if node_run['online'] in [ab[1] for ab in Get_AbnormalNodeRuns(DATA_LIST)]:
            print("Skip abnormal sample: ", node_run['online'])
            continue

        time = node_run['online'].replace(":", "_").replace(" ", "_").replace("-", "_")
        machine_id = node_run['salad_machine_id']
        file_prefix = f"{time}_{machine_id}"

        messages = {
            'Uptime_H':    round(node_run['uptime_s']/3600, 2),
            "CUDA":        node_run['gpu_cuda_version'],
            "GPU":         node_run['gpu_type'].replace("NVIDIA", "").replace("GeForce", ""),
            "Location":    node_run['location'],
            "Country":     node_run['country'],
        }

        Plot_Performance_Single( node_run['history'], file_prefix, "output_normal", messages )


def Plot_Performance_Variance(gpu_type, file_name):
    performance_data = Get_Performance_Variance(DATA_LIST, gpu_type)

    if not performance_data:
        print(f"No performance data to plot for GPU type: {gpu_type}")
        return

    performances, online_times = zip(*performance_data)
    indices = list(range(len(performances)))
    total = len(performances)

    # --- Stats ---
    avg_perf = sum(performances) / total
    below_avg_count = sum(1 for p in performances if p < avg_perf)
    zero_count = sum(1 for p in performances if p == 0)

    below_avg_pct = (below_avg_count / total) * 100
    zero_pct = (zero_count / total) * 100

    # --- Plot ---
    plt.figure(figsize=(15, 6))
    plt.scatter(indices, performances, alpha=0.7)
    plt.axhline(avg_perf, color="red", linestyle="dashed", linewidth=1, label=f"Avg = {avg_perf:.2f}")
    plt.xlabel("Sample Index")
    plt.ylabel("Average Performance (sol/s)")
    plt.title(f"Performance Variance for {gpu_type} ({total} samples)")
    plt.legend()
    plt.grid(True)

    # --- Stats in image ---
    stats_text = (
        f"Samples: {total}\n"
        f"Average: {avg_perf:.2f} sol/s\n"
        f"Below Average: {below_avg_count} ({below_avg_pct:.1f}%)\n"
        f"Zero Perf: {zero_count} ({zero_pct:.1f}%)"
    )
    plt.text(
        0.99, 0.01, stats_text,
        transform=plt.gca().transAxes,
        ha="right", va="bottom",
        fontsize=10,
        bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.7, edgecolor="gray")
    )

    plt.tight_layout()
    plt.savefig(file_name, dpi=600)
    plt.show()


def Plot_Performance_Variance1(gpu_type, file_name):
    performance_data = Get_Performance_Variance(DATA_LIST, gpu_type)

    if not performance_data:
        print(f"No performance data to plot for GPU type: {gpu_type}")
        return

    performances, online_times = zip(*performance_data)
    indices = list(range(len(performances)))

    plt.figure(figsize=(15, 6))
    plt.scatter(indices, performances, alpha=0.7)
    plt.xlabel('Sample Index')
    plt.ylabel('Average Performance (sol/s)')
    plt.title(f'Performance Variance for {gpu_type} ({len(performances)} samples)')
    plt.grid(True)

    plt.tight_layout()
    plt.savefig(file_name, dpi=600)
    plt.show()




if __name__ == "__main__":


    print("----> Plotting Startup times")
    start_time = TIMESTAMP_START
    end_time = start_time + TIME_INTERVAL_1HOUR * 2
    Plot_Startup_Times(end_time, "output/1011_startup_times.png")

    print("----> Plotting Node Run to Request Ratio")
    start_time = TIMESTAMP_START
    end_time = start_time + TIME_INTERVAL_1DAY * 7
    Plot_Node_Run_to_Request_Ratio(start_time, end_time, "Instance Run-to-Request Ratio for 100 Replicas", "output/1021_run_to_request_ratio.png") 

    print("----> Plotting Node Run to Request Ratio (skip first 2 hours)")
    start_time = TIMESTAMP_START + TIME_INTERVAL_1HOUR * 2
    end_time = start_time + TIME_INTERVAL_1DAY * 7
    Plot_Node_Run_to_Request_Ratio(start_time, end_time, "Instance Run-to-Request Ratio for 100 Replicas (skip first 2 hours)", "output/1022_run_to_request_ratio_skip2.png") 

    print("----> Plotting Hourly Instance Allocations")
    start = TIMESTAMP_START 
    end = start + TIME_INTERVAL_1DAY * 7
    interval = TIME_INTERVAL_1HOUR
    Plot_Allocation(start, end, interval, "Hourly Instance Allocations for 100 Replicas", "output/1031_instance_allocation_1hour.png")

    print("----> Plotting Hourly Instance Allocations (skip first 2 hours)")
    start = TIMESTAMP_START + TIME_INTERVAL_1HOUR * 2
    end = start + TIME_INTERVAL_1DAY * 7
    interval = TIME_INTERVAL_1HOUR
    Plot_Allocation(start, end, interval, "Hourly Instance Allocations for 100 Replicas (skip first 2 hours)", "output/1032_instance_allocation_1hour_skip2.png")

    print("----> Plotting Daily Instance Allocations (skip first 2 hours)")
    start = TIMESTAMP_START + TIME_INTERVAL_1HOUR * 2
    end = start + TIME_INTERVAL_1DAY * 7
    interval = TIME_INTERVAL_1DAY
    Plot_Allocation(start, end, interval, "Daily Instance Allocations for 100 Replicas (skip first 2 hours)", "output/1033_instance_allocation_1day_skip2.png")

    start_time = TIMESTAMP_START
    end_time = start_time + TIME_INTERVAL_1DAY * 7
    print("----> Plotting Instance Uptimes - All Instances")
    Plot_Uptime_Distribution(start_time, end_time, 'all',     'all', "Instance Uptime Distribution - All Instances",                     "output/1041_uptime_distribution_all_all.png")
    print("----> Plotting Instance Uptimes - Stopped Instances")
    Plot_Uptime_Distribution(start_time, end_time, 'stopped', 'all', "Instance Uptime Distribution - Stopped Instances",                 "output/1042_uptime_distribution_stopped_all.png")
    print("----> Plotting Instance Uptimes - Running Instances")
    Plot_Uptime_Distribution(start_time, end_time, 'running', 'all', "Instance Uptime Distribution - Running Instances",                 "output/1043_uptime_distribution_running_all.png")
    print("----> Plotting Instance Uptimes - All Instances, high-end GPUs")
    Plot_Uptime_Distribution(start_time, end_time, 'all',     'high', "Instance Uptime Distribution - All Instances, high-end GPUs",     "output/1044_uptime_distribution_all_high.png")
    print("----> Plotting Instance Uptimes - Stopped Instances, high-end GPUs")
    Plot_Uptime_Distribution(start_time, end_time, 'stopped', 'high', "Instance Uptime Distribution - Stopped Instances, high-end GPUs", "output/1045_uptime_distribution_stopped_high.png")
    print("----> Plotting Instance Uptimes - Running Instances, high-end GPUs")
    Plot_Uptime_Distribution(start_time, end_time, 'running', 'high', "Instance Uptime Distribution - Running Instances, high-end GPUs", "output/1046_uptime_distribution_running_high.png")
    print("----> Plotting Instance Uptimes - All Instances, low-end GPUs")
    Plot_Uptime_Distribution(start_time, end_time, 'all',     'low', "Instance Uptime Distribution - All Instances, low-end GPUs",       "output/1047_uptime_distribution_all_low.png")
    print("----> Plotting Instance Uptimes - Stopped Instances, low-end GPUs")
    Plot_Uptime_Distribution(start_time, end_time, 'stopped', 'low', "Instance Uptime Distribution - Stopped Instances, low-end GPUs",   "output/1048_uptime_distribution_stopped_low.png")
    print("----> Plotting Instance Uptimes - Running Instances, low-end GPUs")
    Plot_Uptime_Distribution(start_time, end_time, 'running', 'low', "Instance Uptime Distribution - Running Instances, low-end GPUs",   "output/1049_uptime_distribution_running_low.png")

    print("----> Plotting performance data samples for NVIDIA GeForce RTX 3060:")
    Plot_Performance_Variance("NVIDIA GeForce RTX 3060", "output/performance_variance_3060.png")  
    print("----> Plotting performance data samples for NVIDIA GeForce RTX 4060 Ti:")
    Plot_Performance_Variance("NVIDIA GeForce RTX 4060 Ti", "output/performance_variance_4060ti.png")  
    print("----> Plotting performance data samples for NVIDIA GeForce RTX 3080 Ti:")
    Plot_Performance_Variance("NVIDIA GeForce RTX 3080 Ti", "output/performance_variance_3080ti.png")  
    print("----> Plotting performance data samples for NVIDIA GeForce RTX 5090:")
    Plot_Performance_Variance("NVIDIA GeForce RTX 5090", "output/performance_variance_5090.png")  

    print("----> Plotting Normal Samples")
    Plot_Normal_Samples(20)
    
    print("----> Plotting Abnormal Samples")
    Plot_Abnormal_Samples()
    


    os._exit(0)






