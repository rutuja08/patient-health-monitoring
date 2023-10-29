from patient_record import PatientRecord
from copy import deepcopy
import datetime
import time
import json
import boto3
import os
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.info("This is an info log")


###General Methods###
def validate_updates(record):
    entry1 = deepcopy(record.get_entry())
    record.update_entry()
    entry2 = record.get_entry()
    for k, v in entry1.items():
        if v == entry2[k]:
            print("Both {} are the same:{}".format(k, v))
        else:
            print("\n{} are not the same:\nR1:{}\nR2:{}".format(k, v, entry2[k]))

    return


def create_initial_patient_records(n=10):
    records = []
    for i in range(n):
        records.append(PatientRecord())
    return records


def combine_records(records, object_name):
    record_entries = []

    for record in records:
        record_entries.append(record.entry)

    col_names = record_entries[0].keys()
    time_stamp_no_spaces = "-".join(str(datetime.datetime.now()).split(" "))
    time_stamp_no_colon = "-".join(time_stamp_no_spaces.split(":"))
    filepath = object_name + "-" + time_stamp_no_colon + ".json"

    with open(filepath, 'w') as f:
        json.dump(record_entries, f, default=str)

    logger.info("Local JSON file created: %s", filepath)
    return filepath


def send_records(records, object_name, bucket_name):
    s3 = boto3.client("s3")

    # Ensure 'data_request/' prefix is present for S3 key
    s3_object_key = object_name if object_name.startswith('data_request/') else 'data_request/' + object_name

    # # Assuming 'combine_records' returns a file path
    records_filepath = combine_records(records, object_name)
    s3_object_key = f"{s3_object_key}.json"

    try:
        with open(records_filepath, "rb") as f:
            s3.upload_fileobj(f, bucket_name, s3_object_key)
        logger.info("File uploaded successfully to S3: %s/%s", bucket_name, s3_object_key)
    except FileNotFoundError:
        logger.error("File not found: %s", records_filepath)
        return False
    except boto3.exceptions.NoCredentialsError:
        logger.error("AWS credentials not available")
        return False
    except Exception as e:
        logger.error("Unexpected error occurred: %s", str(e))
        return False
    finally:
        if os.path.exists(records_filepath):
            os.remove(records_filepath)
            logger.info("Local file deleted: %s", records_filepath)

    return True


def simulate_patient_monitoring(patient_records, object_name="patient_records",
                                bucket_name='ruth-hosp', runtime=600, interval=30):
    start_time = time.time()
    interval_time = time.time()
    elapsed_time = 0

    while elapsed_time < runtime:
        for record in patient_records:
            record.update_entry()

        if time.time() - interval_time > interval:
            send_records(patient_records, object_name, bucket_name)
            interval_time = time.time()

        time.sleep(0.5)
        elapsed_time = time.time() - start_time

    return patient_records
