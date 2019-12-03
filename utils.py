r"""
utils.py is a set of helper functions used throughout the transis-kinesis-connection service.
"""
import datetime
import pytz
import os
import boto3
import base64
import json
import time
from botocore.exceptions import ClientError
import logging
log = logging.getLogger(__name__)

def get_formatted_current_timestamp():
    """returns a string representation the current timestamp in the sydney time e.g. 2019-10-18T21:43:32+11:00"""
    now = datetime.datetime.now(pytz.timezone('Australia/Sydney')).strftime("%Y-%m-%dT%H:%M:%S%z")
    now_reformatted = now[:-2] + ':' + now[-2:]
    return now_reformatted

def get_epoc_from_timestamp_string(timestamp):
    """converts a utc epoc with a timezone into a unix timestamp
    
    Arguments:
        timestamp {str} -- e.g. 2019-10-03T15:43:00+10:00
    Returns:
        epoc {int} -- the epoc respresentaion of the given timestamp
    """
    timestamp_ = datetime.datetime.strptime(timestamp,"%Y-%m-%dT%H:%M:%S%z")
    ts = (timestamp_ - datetime.datetime(1970, 1, 1, tzinfo=pytz.utc)).total_seconds()
    return int(ts)

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

def get_config():
    """Gets configurations and passwords for all the services used in the connector."""
    try:
        transis_prod_secrets = json.loads(get_secret("nai/scats/transis"))
        di_framework_secrets = json.loads(get_secret("dev/nai/scats/transis/di-framwork"))

        config = {
            "transis_config_prod" : transis_prod_secrets,
            "di_framework_config" : {
                "connection_details" : {
                    "host": di_framework_secrets["host"],
                    "database": "postgres",
                    "user": di_framework_secrets["username"],
                    "password": di_framework_secrets["password"],
                },
                "schema_name": os.environ['DI_FRAMEWORK_SCHEMA_NAME'],
                "job_name": os.environ['DI_FRAMEWORK_JOB_NAME']
            },
            "kinesis_config": {
                "region_name": os.environ['KINESIS_REGION_NAME'],
                "stream_name" : os.environ['KINESIS_STREAM_NAME']
            }
        }
    except Exception as e:
        log.error(f"The following error occured when attemping to get the credentials and config - will attempt to read from local_config.json: {e}")
        with open("local_config.json","r") as file_handle: 
            config = json.loads(file_handle.read())

    return config

def get_secret(secret_name, region_name="ap-southeast-2"):
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']

        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
    
    if not secret:
        secret = decoded_binary_secret
    return secret
