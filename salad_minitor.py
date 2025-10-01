
import os
import time
import boto3
import sys
import json
from boto3.s3.transfer import TransferConfig
from dotenv import load_dotenv
from collections import defaultdict

load_dotenv()

AWS_ACCESS_KEY_ID      = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY  = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_ENDPOINT_URL       = os.getenv("AWS_ENDPOINT_URL")
AWS_REGION             = os.getenv("AWS_REGION")

BUCKET     = os.getenv("BUCKET")
PREFIX     = os.getenv("PREFIX","")
FOLDER     = os.getenv("FOLDER")

S3Client = boto3.client(
    "s3",
    endpoint_url=AWS_ENDPOINT_URL,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION 
)

g_files = []

# Ensure "data" subfolder exists
os.makedirs("data", exist_ok=True)

# Function to list files in the global g_files list
def listFiles():
    for i in range(len(g_files)):   
        print(f"{i}: {g_files[i]}")


# Function to fetch files from the bucket and store them in the g_files 
def fetchFiles():
    global g_files

    g_files = []  # Reset the global list to avoid duplicates
    
    if not BUCKET or not FOLDER:
        print("Missing BUCKET or FOLDER")
        return []

    paginator = S3Client.get_paginator('list_objects_v2')
    files = []

    if PREFIX == "":
        Prefix=FOLDER
    else:
        Prefix=PREFIX + "/" + FOLDER
    
    for page in paginator.paginate(
        Bucket=BUCKET,
        Prefix=Prefix    
    ):
        for obj in page.get('Contents', []):
            # Remove the folder prefix from the key if you want just the file name
            key = obj['Key']
            if key.endswith('/'):
                continue  # skip folder itself
            files.append(key)
            g_files.append(key.split('/')[-1])  # store just the file name    
    print(f"----> Fetched {len( g_files )} files from R2:{BUCKET}/{Prefix}/")


def downloadFile(file_name):
    if not BUCKET or not FOLDER:
        print("Missing BUCKET or FOLDER")
        return
    
    if PREFIX != "":
        key = f"{PREFIX}/{FOLDER}/{file_name}"
    else:
        key = f"{FOLDER}/{file_name}"

    try:
        response =  S3Client.get_object(Bucket=BUCKET, Key=key)
        content = response['Body'].read().decode('utf-8')
        
        #print(f"\nContent of {file_name}: \n{content}")

        # Save to local file inside "data/"
        local_path = os.path.join("data", file_name)
        with open(local_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"Saved to local file: {local_path}")

    except Exception as e:
        print(f"Failed to get {key}: {e}")


def showFile(file_name):
    if not BUCKET or not FOLDER:
        print("Missing BUCKET or FOLDER")
        return
    
    if PREFIX != "":
        key = f"{PREFIX}/{FOLDER}/{file_name}"
    else:
        key = f"{FOLDER}/{file_name}"

    try:
        response =  S3Client.get_object(Bucket=BUCKET, Key=key)
        content = response['Body'].read().decode('utf-8')
        
        print(f"\nContent of {file_name}: \n{content}")

        # Save to local file inside "data/"
        local_path = os.path.join("data", file_name)
        with open(local_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"Saved to local file: {local_path}")

    except Exception as e:
        print(f"Failed to get {key}: {e}")


def resetFolder():

    if not BUCKET  or not FOLDER:
        print("Missing BUCKET or FOLDER")
        return []

    paginator = S3Client.get_paginator('list_objects_v2')
    to_delete = []

    if PREFIX == "":
        Prefix=FOLDER
    else:
        Prefix=PREFIX + "/" + FOLDER
    
    for page in paginator.paginate(
        Bucket=BUCKET,
        Prefix=Prefix
    ):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('/'):
                continue  # skip folder itself
            else:
                to_delete.append({'Key': key})

    if to_delete:
        # S3 allows deleting up to 1000 objects per request
        for i in range(0, len(to_delete), 1000):
            S3Client.delete_objects(
                Bucket=BUCKET,
                Delete={'Objects': to_delete[i:i+1000]}
            )
        print(f"----> Deleted {len(to_delete)} objects from R2:{BUCKET}/{Prefix}/")
    else:
        print("----> No objects to delete.")
    

def run():
    global FOLDER

    while True:
        print(f"\nThe remote folder - R2:{BUCKET}/{FOLDER}/")
        print(f"The local folder - ./data/")
        cmd = input("Enter (file ID, f-fetch new files, l-list, d-fetch and download all, reset-purge (inactive), e-exit): ").strip()

        if not cmd:
            continue
        if cmd == "f":
            fetchFiles()
            listFiles()
        elif cmd == "l":
            listFiles()
        elif cmd == "d":
            fetchFiles()
            for x in g_files:
                downloadFile(x)
                time.sleep(0.5)
        #elif cmd == "reset":
        #    resetFolder()
        elif cmd == "e":
            print("Exiting.")
            exit(0)
        elif cmd.isdigit():
            if int(cmd) < len(g_files):
                showFile( g_files[int(cmd)])
        else:
            continue

if __name__ == "__main__":
    run()