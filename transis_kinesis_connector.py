import requests
import boto3
from transis_response_models import TransisResponse
import di_framework
import json
import logging

log = logging.getLogger(__name__)

class TransisKinesisConnector:
    """ Represents the adaptor between transis and kinesis"""

    def __init__(self,transis_consumer,kinesis_producer, di_framework_client):
        self.transis_consumer = transis_consumer
        self.kinesis_producer = kinesis_producer
        self.di_framework_client = di_framework_client


    def run(self):
        """Processes the transis responses managing the starting, ending and logging of DI jobs"""
        for transis_response in self.transis_consumer.get_detector_counts():
            self.di_framework_client.start_job()
            response = self.push_transis_response_to_kinesis(transis_response, self.di_framework_client)
            log.info(response)
            self.di_framework_client.log_job_status(json.dumps(response))
            self.di_framework_client.end_job()            
    
    def push_transis_response_to_kinesis(self, transis_response, di_framework_client):
        """Pushes the list of detector count messages after thier appropriate transformation, to be sent to kinesis
        
        Arguments:
            transis_response {TransisResponse} -- the detector count response recieved from transis
        
        Returns:
            {Dict} -- Details about how many records where processed
        """
        detector_count_messages = transis_response.detector_count_messages.detector_count_message_list
        records = [e.to_dict() for e in detector_count_messages]
        self.kinesis_producer.push_transis_detector_count_records(records, di_framework_client)
        return {
            "records_in_xml_doc": len(records),
            "collectionendtimestamp_plus_3_mins": detector_count_messages[0].collectionendtimestamp_plus_3_mins,
            "response_received_timestamp": transis_response.response_received_timestamp
        }        