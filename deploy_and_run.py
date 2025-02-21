#!/usr/bin/env python
"""
Deployment Script:
- Loads configuration (with defaults) from environment variables.
- Optionally loads a .env file using python-dotenv.
- Zips the extra Python files (all in scripts/ except main.py), uploads code to S3,
  creates/updates an AWS Glue job, and triggers its run.
"""

import os
import zipfile
import boto3
import sys
import time
from botocore.exceptions import ClientError
import stat
import subprocess

# Set deploy.sh as executable
deploy_script = "deploy.sh"
os.chmod(deploy_script, os.stat(deploy_script).st_mode | stat.S_IEXEC)
print(f"{deploy_script} is now executable.")


# # Optionally load .env if present
# try:
#     from dotenv import load_dotenv
#     load_dotenv()
# except ImportError:
#     pass  # Defaults will be used if python-dotenv is not installed

# ======================
# Configuration (with Defaults)
# ======================

AWS_REGION       = os.environ.get("AWS_REGION", "us-east-1")
S3_BUCKET        = os.environ.get("S3_BUCKET", "bamma0927")
S3_CODE_PREFIX   = os.environ.get("S3_CODE_PREFIX", "aws-glue-code")
JOB_NAME         = os.environ.get("JOB_NAME", "prod-transformation-job")
IAM_ROLE         = os.environ.get("IAM_ROLE", "AWSGlueServiceRole-MyRole")
GLUE_VERSION     = os.environ.get("GLUE_VERSION", "3.0")
SOURCE_PATH      = os.environ.get("SOURCE_PATH", "s3://bamma0927/source_data/")
TARGET_PATH      = os.environ.get("TARGET_PATH", "s3://bamma0927/transformed_data/")
PROJECT_DIR      = os.environ.get("PROJECT_DIR", "aws-glue-project")

# Local paths (assuming repository layout is preserved)
SCRIPTS_DIR = os.path.join(PROJECT_DIR, "scripts")
MAIN_SCRIPT_LOCAL = os.path.join(SCRIPTS_DIR, "main.py")
DEPENDENCIES_ZIP_LOCAL = "dependencies.zip"  # Zip file for extra modules

# ======================
# Load extra modules from requirements.txt
# ======================

def load_extra_modules(requirements_file="requirements.txt"):
    """
    Reads the requirements file and returns a comma-separated list
    of Python modules for Glue's --additional-python-modules parameter.
    """
    modules = []
    if os.path.exists(requirements_file):
        with open(requirements_file, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    modules.append(line)
    else:
        print(f"Requirements file {requirements_file} not found.")
    return ",".join(modules)

EXTRA_PY_MODULES = load_extra_modules("requirements.txt")
print(f"Extra Python modules from requirements.txt: {EXTRA_PY_MODULES}")

# ======================
# Helper Functions
# ======================

def zip_dependencies():
    """
    Zips all Python files from the scripts folder (except main.py) into a zip file.
    This zip file is used as extra Python files (dependencies) for the Glue job.
    """
    with zipfile.ZipFile(DEPENDENCIES_ZIP_LOCAL, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(SCRIPTS_DIR):
            for file in files:
                if file.endswith(".py") and file != "main.py":
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, SCRIPTS_DIR)
                    print(f"Adding {file_path} as {arcname}")
                    zipf.write(file_path, arcname)
    print(f"Created zip file: {DEPENDENCIES_ZIP_LOCAL}")

def upload_file_to_s3(local_path, bucket, s3_key):
    """
    Uploads a local file to the specified S3 bucket and key.
    """
    s3 = boto3.client("s3", region_name=AWS_REGION)
    try:
        s3.upload_file(local_path, bucket, s3_key)
        print(f"Uploaded {local_path} to s3://{bucket}/{s3_key}")
    except ClientError as e:
        print(f"Error uploading {local_path} to s3://{bucket}/{s3_key}: {e}")
        sys.exit(1)

def create_or_update_glue_job(job_name, script_s3_path, extra_py_files_s3_path):
    """
    Creates or updates the AWS Glue job with the given parameters.
    """
    glue = boto3.client("glue", region_name=AWS_REGION)
    job_command = {
        "Name": "glueetl",
        "ScriptLocation": script_s3_path,
        "PythonVersion": "3"
    }
    
    default_arguments = {
        "--extra-py-files": extra_py_files_s3_path,
        "--job-language": "python",
        "--additional-python-modules": EXTRA_PY_MODULES,
        "--source_path": SOURCE_PATH,
        "--target_path": TARGET_PATH,
    }
    
    job_args = {
        "Name": job_name,
        "Role": IAM_ROLE,
        "Command": job_command,
        "DefaultArguments": default_arguments,
        "GlueVersion": GLUE_VERSION,
        "MaxRetries": 0,
        "Timeout": 2880,
        "NumberOfWorkers": 2,
        "WorkerType": "G.1X"
    }
    
    try:
        glue.get_job(JobName=job_name)
        print(f"Job {job_name} exists. Updating job.")
        update_args = job_args.copy()
        update_args.pop("Name", None)
        glue.update_job(JobName=job_name, JobUpdate=update_args)
    except glue.exceptions.EntityNotFoundException:
        print(f"Job {job_name} does not exist. Creating job.")
        glue.create_job(**job_args)

def start_glue_job(job_name):
    """
    Starts the Glue job run and monitors its status until completion.
    """
    glue = boto3.client("glue", region_name=AWS_REGION)
    try:
        response = glue.start_job_run(JobName=job_name)
        job_run_id = response["JobRunId"]
        print(f"Started job run: {job_run_id}")
    except ClientError as e:
        print(f"Failed to start job {job_name}: {e}")
        sys.exit(1)
    
    while True:
        try:
            status_response = glue.get_job_run(JobName=job_name, RunId=job_run_id)
            status = status_response["JobRun"]["JobRunState"]
            print(f"Job run {job_run_id} status: {status}")
            if status in ["SUCCEEDED", "FAILED", "STOPPED"]:
                break
            time.sleep(30)
        except ClientError as e:
            print(f"Error getting job run status: {e}")
            time.sleep(30)
    
    if status == "SUCCEEDED":
        print("Job run completed successfully.")
    else:
        print(f"Job run ended with status: {status}")
        sys.exit(1)

def main():
    zip_dependencies()
    
    s3_main_key = f"{S3_CODE_PREFIX}/main.py"
    s3_dep_key  = f"{S3_CODE_PREFIX}/dependencies.zip"
    
    upload_file_to_s3(MAIN_SCRIPT_LOCAL, S3_BUCKET, s3_main_key)
    upload_file_to_s3(DEPENDENCIES_ZIP_LOCAL, S3_BUCKET, s3_dep_key)
    
    s3_main_uri = f"s3://{S3_BUCKET}/{s3_main_key}"
    s3_dep_uri  = f"s3://{S3_BUCKET}/{s3_dep_key}"
    
    create_or_update_glue_job(JOB_NAME, s3_main_uri, s3_dep_uri)
    start_glue_job(JOB_NAME)

if __name__ == "__main__":
    main() #b22
