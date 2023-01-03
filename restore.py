import json
import argparse
from datetime import datetime, timedelta
import os
from pathlib import Path
import requests
from requests_gssapi import HTTPSPNEGOAuth
import urllib3
import subprocess


# Local directory to store the downloaded files temporarily.
LOCAL_DIR = "tmp_logs"


def get_days_ago_date(days_ago):
    """
    Get the date to start downloading files based on how many days ago.
    days_ago -- How many days ago we want to start downloading the logs, for exmaple, put "0" will return today's date, and put "2" will return the date before yesterday.
    """
    datetime_N_days_ago = datetime.now() - timedelta(days=days_ago)
    date_N_days_ago = datetime_N_days_ago.date()

    return date_N_days_ago


def is_date_str(potential_date_str):
    """
    Determine if `potential_date` is a string representing a date, such as "20221213".
    """
    try:
        datetime.strptime(potential_date_str, "%Y%m%d").date()
        return True
    except:
        return False


def is_later_date(potential_date_str, days_ago):
    """
    Determine if it is a date and a date on or after a date.
    Returns False either if potential_date does not represent a date or potential_date is not a later date.
    """
    if not is_date_str(potential_date_str):
        return False

    new_date = datetime.strptime(potential_date_str, "%Y%m%d").date()
    date_N_days_ago = get_days_ago_date(days_ago)
    return new_date >= date_N_days_ago


def get_user_inputs():
    """
    To get customer inputs.
    """
    parser = argparse.ArgumentParser(
        description="A script to download ranger audit logs from the cloud and insert them into Solr.")
    parser.add_argument("--cloud_type", required=True,
                        help="The cloud type, it should be either AWS or AZURE.")
    parser.add_argument("--storage_location", required=True,
                        help="The storage location where the data is stored, without the prefix. (example: my-bucket-name/my-env-name/data)")
    parser.add_argument("--solr_path", required=True,
                        help="The Solr path where we want to insert the content into. (example: my-env0.myname.xcu2-8y8x.wl.cloudera.site:8985)")
    parser.add_argument("--days_ago", required=True, type=int,
                        help="How many days ago we want to start downloading the logs.")
    parser.add_argument("--access_key_id", required=True,
                        help="AWS Access Key ID.")
    parser.add_argument("--secret_access_key", required=False,
                        help="AWS Secret Access Key.")
    args = vars(parser.parse_args())

    return args["cloud_type"], args["storage_location"], args["solr_path"], args["days_ago"], args["access_key_id"], args["secret_access_key"]


def download_s3_folder(s3_location, days_ago, local_dir, access_key_id, secret_access_key):
    """
    Download logs from AWS.
    Keyword arguments:
    s3_location -- The s3 storage location where the data is stored, without the prefix. (example: my-bucket-name/my-env-name/data)
    days_ago -- How many days ago we want to start downloading the logs, for exmaple, put "0" will download today's logs, and put "2" 
    will download the logs of today, yesterday, and the day before yesterday's.
    local_dir -- The local location where we want to store the the downloaded files temporarily, this defaults to "tmp_logs".
    """
    import boto3

    # Get the AWS bucket and the s3 folder path.
    s3_location_list = s3_location.split("/")
    s3_bucket, s3_path = s3_location_list[0], "/".join(s3_location_list[1:])
    s3 = boto3.resource("s3", aws_access_key_id=access_key_id,
                        aws_secret_access_key=secret_access_key)
    bucket = s3.Bucket(s3_bucket)

    # Loop through all the directories under the s3 path.
    for obj in bucket.objects.filter(Prefix=s3_path):
        # Skip directory that is not ranger audit.
        if "ranger/audit" not in obj.key:
            continue

         # Skip if the second to the last folder is not a date folder, for example, it is "/tests".
         # Skip if the second to the last folder is a date that is before the date we want to start downloading,
         # for example, we want to start downloading from "20230101" but the folder is "20221230".
        potential_date_str = obj.key.split("/")[-2]
        if not is_date_str(potential_date_str) or not is_later_date(potential_date_str, days_ago):
            continue

        # Skip if the folder is empty.
        filename = obj.key.split("/")[-1]
        if filename == "":
            continue

        # Create a tmp folder to store the files, an example of the destination: tmp_logs/20230101.
        destination = os.path.join(local_dir, potential_date_str)
        Path(destination).mkdir(parents=True, exist_ok=True)
        bucket.download_file(obj.key, os.path.join(destination, filename))
        print("Downloaded file: " + filename + " to " + destination)


def download_blob_folder(blob_location, days_ago, local_dir, access_key_id):
    """
    Download logs from AZURE.
    Keyword arguments:
    blob_location -- The blob storage location where the data is stored, without the prefix. (example: data@myresourcegroup.dfs.core.windows.net )
    days_ago -- How many days ago we want to start downloading the logs, for exmaple, put "0" will download today's logs, and put "2" 
    will download the logs of today, yesterday, and the day before yesterday's.
    local_dir -- The local location where we want to store the the downloaded files temporarily, this defaults to "tmp_logs".
    """
    from azure.identity import DefaultAzureCredential
    from azure.storage.blob import BlobServiceClient

    # Get the container and account.
    blob_location_list = blob_location.split("@", maxsplit=1)
    container = blob_location_list[0]
    account = blob_location_list[1].split(".")[0]
    account_url = "https://" + account + ".blob.core.windows.net"
    default_credential = DefaultAzureCredential()

    # Create the BlobServiceClient object and container client in order to get the folder list under "ranger/audit".
    blob_service_client = BlobServiceClient(
        account_url, credential=access_key_id)
    container_client = blob_service_client.get_container_client(container)
    blob_list = container_client.list_blobs()

    for blob in blob_list:
        # Skip directory that is not ranger audit.
        if "ranger/audit" not in blob.name:
            continue

        # Skip if the second to the last folder is not a date folder, for example, it is "/tests".
        # Skip if the second to the last folder is a date that is before the date we want to start downloading,
        # for example, we want to start downloading from "20230101" but the folder is "20221230".
        potential_date_str = blob.name.split("/")[-2]
        if not is_date_str(potential_date_str) or not is_later_date(potential_date_str, days_ago):
            continue

        # Skip if the folder is empty.
        filename = blob.name.split("/")[-1]
        if filename == "":
            continue

        # Create a tmp folder to store the files, an example of the destination: tmp_logs/20230101.
        destination = os.path.join(local_dir, potential_date_str)
        Path(destination).mkdir(parents=True, exist_ok=True)
        # Create a local empty file and then we will write the content to it.
        file_path = os.path.join(destination, filename)
        with open(file=file_path, mode="wb") as local_tmp_file:
            local_tmp_file.write(
                container_client.download_blob(blob.name).readall())
            print("Downloaded file: " + filename + " to " + destination)


def read_file_as_json_list(dir_entry):
    """
    Convert json into an array, the format accepted by the Solr API.
    Keyword arguments:
    dir_entry -- The DirEntry of a log file, for example "hdfs_ranger_audit_my-env0.myname.xcu2-8y8x.wl.cloudera.site.log".
    """
    with open(dir_entry, "r") as f:
        file_text = f.read()

    file_text = file_text.rstrip('\n')
    json_str_list = file_text.split('\n')
    json_obj_list = [json.loads(json_str) for json_str in json_str_list]

    return json_obj_list


def send_solr_update_request(solr_path, json_list):
    """
    Call the Solr update API.
    """
    url = 'https://' + solr_path + '/solr/ranger_audits/update'
    return requests.post(
        url,
        params={
            'commitWithin': '1000',
            'overwrite': 'true',
            'wt': 'json',
        },
        headers={
            'Content-Type': 'application/json',
        },
        json=json_list,
        verify=False,
        auth=HTTPSPNEGOAuth(),
    )


def upload_to_solr(local_dir, solr_path):
    """
    Insert the content of the logs into Solr ranger audits collection.
    Keyword arguments:
    local_dir -- The local location where we stored the downloaded files temporarily, this defaults to "tmp_logs".
    solr_path -- The Solr path where we want to insert the content into, and this is a combination of the Solr hostname and Solr HTTPs port, separated with a colon. 
    (example: my-env0.myname.xcu2-8y8x.wl.cloudera.site:8985)
    """
    if not os.path.exists(local_dir):
        return

    for dir in os.scandir(local_dir):
        if not dir.is_dir():
            continue

        # Loop through all log files under the folder.
        for dir_entry in os.scandir(dir):
            if not dir_entry.is_file():
                continue

            json_list = read_file_as_json_list(dir_entry)
            response = send_solr_update_request(solr_path, json_list)
            response.raise_for_status()
            print("Inserted " + dir_entry.name + " into Solr.")


def remove_dir(local_dir):
    """
    Remove the temporary folder that stored the dowloaded logs.
    Keyword arguments:
    local_dir -- The local location where we stored the downloaded files temporarily, this defaults to "tmp_logs".
    """
    if not os.path.exists(local_dir):
        return

    for dir in os.scandir(local_dir):
        if os.path.isdir(dir):
            for filename in os.scandir(dir):
                os.remove(filename)
            os.rmdir(dir)
        else:
            os.remove(dir)
    print("Removed /tmp_logs.")


def main():
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    # Login as a Solr user using Kerberos.
    KEYTAB_COMMAND = '''
    kinit -kt "$(find /run/cloudera-scm-agent/process -name solr.keytab | tail -n 1)" "$(klist -kt "$(find /run/cloudera-scm-agent/process -name solr.keytab | tail -n 1)" | tail -n 1 | awk '{print $4}')"
    '''
    ps = subprocess.Popen(KEYTAB_COMMAND, shell=True,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    _, stderr = ps.communicate()
    if stderr:
        raise RuntimeError('Kerberos command failed.')

    try:
        USER_INPUTS = get_user_inputs()
        CLOUD_TYPE, STORAGE_LOCATION, SOLR_PATH, DAYS_AGO = USER_INPUTS[
            0], USER_INPUTS[1], USER_INPUTS[2], USER_INPUTS[3]

        # Get inputs and download files.
        if CLOUD_TYPE.lower() == "aws":
            ACCESS_KEY_ID, SECRET_ACCESS_KEY = USER_INPUTS[4], USER_INPUTS[5]
            download_s3_folder(STORAGE_LOCATION, DAYS_AGO,
                               LOCAL_DIR, ACCESS_KEY_ID, SECRET_ACCESS_KEY)
        elif CLOUD_TYPE.lower() == "azure":
            ACCESS_KEY_ID = USER_INPUTS[4]
            download_blob_folder(STORAGE_LOCATION, DAYS_AGO,
                                 LOCAL_DIR, ACCESS_KEY_ID)
        else:
            raise ValueError("cloud_type must be either AWS or AZURE.")

        # Insert files into Solr.
        upload_to_solr(LOCAL_DIR, SOLR_PATH)
    except Exception as ex:
        print(ex)
    finally:
        # Always remove the temporary folder as well as all the files.
        remove_dir(LOCAL_DIR)


if __name__ == '__main__':
    main()
