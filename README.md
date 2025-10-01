# performance-reliability-test-2025

This repository contains code for running performance and reliability tests on SaladCloud, gathering key metrics such as instance startup times, interruptions and reallocations, run-to-request ratio, uptimes, and overall performance.

For the final test results, see the [Service Performance and Reliability Overview](https://docs.salad.com/container-engine/tutorials/performance/service-performance).


### Image

The [Image](https://github.com/SaladTechnologies/performance-reliability-test-2025/blob/main/image/Dockerfile) comes with lolMiner pre-integrated. On startup, it launches two threads:

- [Scheduler Thread](https://github.com/SaladTechnologies/performance-reliability-test-2025/blob/main/image/main.py#L115) - Runs a loop that creates a new [Metric_Task thread](https://github.com/SaladTechnologies/performance-reliability-test-2025/blob/main/image/main.py#L70) every 1 minute (METRIC_INTERVAL) to collect metrics. These metrics (stored as a metric file) are added to a local queue (upload_queue) every 5 minutes (REPORT_NUMBER).

- [Uploader THread](https://github.com/SaladTechnologies/performance-reliability-test-2025/blob/main/image/main.py#L128) - Reads metric files from the local queue and uploads the metric file to the cloud (S3-Compatible). 

It then runs lolMiner continuously, redirecting its output to a local file (LOCAL_LOG_FILE).

### Local Test

Prepare a .env file for both local test and deployment on Saladcloud:
```
# Wallet to receive subsidies
WALLET=******

# Access to Cloud Storage
AWS_ENDPOINT_URL=******
AWS_ACCESS_KEY_ID=******
AWS_SECRET_ACCESS_KEY=******
AWS_REGION=******

# The target bucket and prefix/folder
BUCKET=******
PREFIX=******
FOLDER=******

METRIC_INTERVAL=60
REPORT_NUMBER=5

SALAD_MACHINE_ID=local # optional
```

You can use docker compose to start the container defined in [docker-compose.yaml](https://github.com/SaladTechnologies/performance-reliability-test-2025/blob/main/docker-compose.yaml). The command automatically loads environment variables from the .env file in the same directory.

```
docker image build -t docker.io/saladtechnologies/misc:001-performance-test -f Dockerfile .
docker push  docker.io/saladtechnologies/misc:001-performance-test 
docker compose up
docker compose down
```

### Deployment on SaladCloud

We created a container group with 100 replicas, each configured with 8 vCPUs, 16 GB of memory, and all consumer GPU types at high priority. The group ran continuously for 7 days and 2 hours, generating a metric file for each instance run, stored in the designated bucket and folder.

### Monitoring

You can run [salad_monitor.py](https://github.com/SaladTechnologies/performance-reliability-test-2025/blob/main/salad_minitor.py) to monitor the test progress, and download all the uploaded metric files to local (./data). See [the example metric files](https://github.com/SaladTechnologies/performance-reliability-test-2025/tree/main/data) for reference.


### Data Analytics and Virtualization

Run analysis_draw.py to analyze and virualize the metric files in the ./data folder. See [the output files](https://github.com/SaladTechnologies/performance-reliability-test-2025/tree/main/output) for reference.
