r"""
main.py is the entry point for the transis kinesis detector volume connector.
"""

from transis_consumer import TransisConsumer
from kinesis_producer import KinesisProducer
from transis_kinesis_connector import TransisKinesisConnector
import di_framework
import transis_response_models
import requests
import os
import logging
import utils
import boto3

logging.basicConfig(
         format='%(asctime)s %(levelname)-8s [%(filename)-28s:%(lineno)d] %(message)s',
         level=logging.INFO,
         datefmt='%Y-%m-%d %H:%M:%S')
         
def main():
    try:
        config = utils.get_config() # create a ./local_config.json file if you want to run this locally or this will fail
        transis_consumer = TransisConsumer(config["transis_config_prod"])
        
        kinesis_client = boto3.client('kinesis',config["kinesis_config"]["region_name"])
        kinesis_producer = KinesisProducer(config["kinesis_config"]["region_name"],config["kinesis_config"]["stream_name"],kinesis_client)
        di_framework_client = di_framework.DIFramework(config["di_framework_config"])
        transis_kinesis_connector = TransisKinesisConnector(transis_consumer, kinesis_producer, di_framework_client)
        transis_kinesis_connector.run()
    except Exception as e:
        logging.critical(f"shutting down the service as a fatal error has occured: {e}")
        try:
            di_framework_client.close_db_connection()
        except UnboundLocalError:
            pass
        exit()
if __name__ == '__main__':
    main()