import json
import boto3
import csv
import os
import psycopg2
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.info("This is an info log")

# Database configuration
DB_NAME = "health_records"
DB_USER = "postgres"
DB_PASSWORD = "root"
DB_HOST = "localhost"
DB_PORT = "5432"


def evaluate_bp(event):
    # https://www.heart.org/en/health-topics/high-blood-pressure/understanding-blood-pressure-readings
    # Used above site for healthy BP ranges

    logger.info("Evaluating patient BP:")

    systolic = event['Systolic_BP']
    diastolic = event['Diastolic_BP']

    if systolic < 120 and diastolic < 80:
        event['BP_evaluation'] = "normal"

    elif (systolic >= 120 and systolic <= 129) and diastolic < 80:
        event['BP_evaluation'] = "elevated"

    elif (systolic >= 130 and systolic <= 139) or (diastolic >= 80 and diastolic <= 89):
        event['BP_evaluation'] = "Hypertension_Stage_I"

    elif systolic >= 140 or diastolic >= 90:
        event['BP_evaluation'] = "Hypertension_Stage_II"

    elif systolic >= 180 or diastolic >= 120:
        event['BP_evaluation'] = "Hypertensive_Crisis"

    return event


def evaluate_hr(event):
    # https://www.heart.org/en/healthy-living/fitness/fitness-basics/target-heart-rates
    # Used above site for healthy HR ranges

    logger.info("Evaluating patient heart rate:")

    heart_rate = event['Heart_Rate']

    if heart_rate >= 60 and heart_rate <= 100:
        event['Heart_Rate_Eval'] = "normal"

    elif heart_rate < 60:
        event["Heart_Rate_Eval"] = "low"

    else:
        event["Heart_Rate_Eval"] = "high"

    return event


def evaluate_blood_sugar(event):
    # https://www.webmd.com/diabetes/how-sugar-affects-diabetes
    # Used the above site to get rough numbers for this.
    # There didn't seem to be a general consensus on this, so I took some
    # liberties here based on family experience.

    logger.info("Evaluating patient blood sugar:")

    blood_sugar = event['Blood_Sugar']

    if blood_sugar < 70:
        event["Blood_Sugar_Eval"] = "low"

    elif blood_sugar >= 180:
        event["Blood_Sugar_Eval"] = "high"

    else:
        event["Blood_Sugar_Eval"] = "normal"

    return event


def evaluate_blood_oxygen(event):
    # https://www.health.state.mn.us/diseases/coronavirus/pulseoximeter.html
    # Used the above site for healthy blood oxygen ranges

    logger.info("Evaluating patient blood oxygen levels:")

    blood_oxygen = event["Blood_Oxygen"]

    if blood_oxygen < 95:
        event["Blood_Oxygen_Eval"] = "low"

    else:
        event["Blood_Oxygen_Eval"] = "normal"

    return event


def evaluate_body_temp(event):
    # https://www.mayoclinic.org/first-aid/first-aid-fever/basics/art-20056685
    # Used the above site for healthy body temperature ranges

    logger.info("Evaluating patient body temperature:")

    body_temp = event["Body_Temperature"]

    if body_temp < 97:
        event["Body_Temp_Eval"] = "low"

    elif body_temp > 99:
        event["Body_Temp_Eval"] = "high"

    else:
        event["Body_Temp_Eval"] = "normal"

    return event


def evaluate_patient(event_body):
    logger.info("Evaluating patient metrics:")

    for event in event_body:
        event = evaluate_bp(event)
        event = evaluate_hr(event)
        event = evaluate_blood_sugar(event)
        event = evaluate_blood_oxygen(event)
        event = evaluate_body_temp(event)

    return event_body


def convert_to_csv(record_entries, object_name):
    col_names = record_entries[0].keys()
    filepath = '/tmp/' + object_name
    with open(filepath, 'w', newline='') as csvFile:
        wr = csv.DictWriter(csvFile, fieldnames=col_names)
        wr.writeheader()
        wr.writerows(record_entries)
    return filepath


def send_to_processed_bucket(csv_filepath, bucket_name, output_folder):
    s3 = boto3.client("s3")
    file_name = os.path.basename(csv_filepath)
    s3_key = f"{output_folder}/{file_name}"
    s3.upload_file(csv_filepath, bucket_name, s3_key)
    logger.info(f"File uploaded to s3://{bucket_name}/{s3_key}")
    return


def save_to_postgres(data):
    connection = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
    cursor = connection.cursor()

    # Insert query template
    insert_query = """ 
    INSERT INTO patient_data (
        Record_ID, Patient_ID, First_Name, Last_Name, DOB, Check_In_Date, 
        Record_Timestamp, Systolic_BP, Diastolic_BP, Heart_Rate, 
        Body_Temperature, Blood_Oxygen, Blood_Sugar, BP_evaluation, 
        Heart_Rate_Eval, Blood_Sugar_Eval, Blood_Oxygen_Eval, Body_Temp_Eval
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    # Data extraction
    for event in data:
        values = (
            event['Record_ID'],
            event['Patient_ID'],
            event['First_Name'],
            event['Last_Name'],
            event['DOB'],
            event['Check_In_Date'],
            event['Record_Timestamp'],
            event['Systolic_BP'],
            event['Diastolic_BP'],
            event['Heart_Rate'],
            event['Body_Temperature'],
            event['Blood_Oxygen'],
            event['Blood_Sugar'],
            event['BP_evaluation'],
            event['Heart_Rate_Eval'],
            event['Blood_Sugar_Eval'],
            event['Blood_Oxygen_Eval'],
            event['Body_Temp_Eval']
        )
        cursor.execute(insert_query, values)

    # Commit changes and close the connection
    connection.commit()
    cursor.close()
    connection.close()


def lambda_handler(bucket_name):
    output_folder = 'output'
    s3_client = boto3.client('s3')

    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix='data_request/', MaxKeys=1000)
    if 'Contents' not in response:
        logger.info("No files found in 'data_request/'")
        return

    for content in response['Contents']:
        key = content['Key']
        if key.endswith('/') or not key.endswith('.json'):
            logger.info("Skipping: %s", key)
            continue

        s3_resource = boto3.resource('s3')
        obj = s3_resource.Object(bucket_name, key)
        file_content = obj.get()['Body'].read().decode('utf-8')
        if not file_content:
            logger.error("File is empty: %s", key)
            continue

        try:
            event_body = json.loads(file_content)
            logger.info("File content:")
            logger.info(event_body)

            processed_events = evaluate_patient(event_body)
            logger.info("Patient Evaluation finished.  Results:")
            logger.info(processed_events)

            object_name = key.split('/')[-1].replace('.json', '.csv')
            csv_filepath = convert_to_csv(processed_events, object_name)
            send_to_processed_bucket(csv_filepath, bucket_name, output_folder)
            save_to_postgres(processed_events)
        except json.JSONDecodeError:
            logger.error("File is not in JSON format or is corrupted: %s", key)

    return {
        'statusCode': 200,
        'body': json.dumps("Processing complete")
    }


if __name__ == "__main__":
    bucket_name = 'ruth-hosp'
    lambda_handler(bucket_name)