import os
import re
import glob
import shutil
import traceback
import argparse

from pathlib import Path
from google.cloud import storage
from google.cloud.storage import transfer_manager
from google.api_core.exceptions import PreconditionFailed

def copy_jars(root_dir:str, env:str):
    storage_client = storage.Client()
    bucket_name = "dataflow-uber-jars"
    print(f"Bucket name: {bucket_name}")
    bucket = storage_client.bucket(bucket_name)
    directory_as_path_obj = Path(root_dir)
    #jar_files = directory_as_path_obj.rglob("*/*/*/*.jar")
    jar_files = directory_as_path_obj.rglob("*.jar")
    #jar_files = glob.glob(f"{root_dir.rstrip('/')}/*/*/*/*.jar", recursive=True)
    #pipeline_jars = [jar_file for jar_file in jar_files if not re.search("(original|common)", jar_file.split("/")[-1])]
    pipeline_jars = [jar_file for jar_file in jar_files if not re.search("(original|common)", jar_file.name)]
    relative_paths_for_jars = [str(path.relative_to(root_dir)) for path in pipeline_jars]
    latest_jars_path = f"{root_dir}/latest/{env}"
    # Create latest jars directory if it doesnt exist
    os.makedirs(f"{root_dir}/latest/{env}", exist_ok=True)
    for filename in os.listdir(root_dir):
        src_file = os.path.join(root_dir, filename)
        dst_file = os.path.join(latest_jars_path, re.sub("-\d\.\d(\.\d)?","", filename))
        if os.path.isfile(src_file):
            shutil.copy(src_file, dst_file)
            print(f"Copied {src_file} to {dst_file}")
    relative_paths_for_latest_jars = [f"latest/{env}/"+re.sub("-\d\.\d(\.\d)?","", path) for path in relative_paths_for_jars]
    #relative_paths_for_jars.extend(relative_paths_for_latest_jars)
    print(relative_paths_for_jars)
    print(relative_paths_for_latest_jars)
    # Start the upload. Transfet manager uploads the files concurrently
    
    # First uploading the versioned jar files to <BUCKET>
    # skip_if_exists should be True for versioned jar files, so it will give error when overriding same version jars
    print("#### Uploading the versioned jar file(s) ######")
    results = transfer_manager.upload_many_from_filenames(
        bucket, relative_paths_for_jars, source_directory=root_dir, skip_if_exists=True, max_workers=8
    )
    for name, result in zip(relative_paths_for_jars, results):
        if isinstance(result, Exception):
            raise RuntimeError("Failed to upload {} due to exception: {}".format(name, result))
        print("Uploaded {} to {}.".format(name, bucket.name))
    
    # Now uploading the version removed jars to <BUCKET>/latest/<ENV>
    # skip_if_exists should be False for un-versioned jar files, so it will override with latest jars
    print("#### Uploading the un-versioned jar file(s) ######")
    results = transfer_manager.upload_many_from_filenames(
        bucket, relative_paths_for_latest_jars, source_directory=root_dir, skip_if_exists=False, max_workers=8
    )
    for name, result in zip(relative_paths_for_latest_jars, results):
        if isinstance(result, Exception):
            raise RuntimeError("Failed to upload {} due to exception: {}".format(name, result))
        print("Uploaded {} to {}.".format(name, bucket.name))

def get_env_from_branch(branch_name:str):
    if branch_name == "main":
        return "prod"
    elif branch_name == "develop/uat":
        return "uat"
    elif branch_name == "develop/test":
        return "test"
    elif branch_name == "develop/dev":
        return "dev"
    else:
        return branch_name

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--branch", help="should be one of develop/dev,develop/test,develop/uat,main", metavar="",  required=True)
    #parser.add_argument("--tenant", help="tenant id", metavar="",  required=True)
    #parser.add_argument("--pipeline", help="pipeline should be one of sourcing, ingestion", metavar="",  required=True)
    parser.add_argument("--jar_type", help="jar type like gcs-to-bigquery etc", metavar="",  required=True)
    parser.add_argument("--jars_directory", help="Root directory of dataflow source code", required=True)
    args = parser.parse_args()
    env = get_env_from_branch(args.branch)

    copy_jars(args.jars_directory, env)