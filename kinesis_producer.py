r"""
kinesis_producer.py is responsible for pushing records to kinesis, handling rate limites and the kinesis connection.
"""
import boto3
import json
import time
import utils
import logging
log = logging.getLogger(__name__)

class KinesisProducer:
    """The AWS kinesis client responsible for pushing records to kinesis, handling rate limites and the kinesis connection. 

    Attributes:
        region         (str)         : AWS region for where the kinesis services is running
        stream_name    (str)         : Name of the kinesis stream
        kinesis_client (boto3.client): boto3 kinesis client object used to interface with the kinesis service
    """

    def __init__(self,region,stream_name,kinesis_client):
        self.region = region
        self.stream_name = stream_name
        self.kinesis_client = kinesis_client
    
    def push_transis_detector_count_records(self, records, di_framework_client, batch_size=10, partition_key="region"):
        """Batches and decorates a list of records to be pushed into kinesis

        Note:
            A batch_size of 10 is about half way to hitting the 1000 records/ sec kineses rate limit for one shard. 
            As the transis data comes in batches every 5 minutes this is more than enough to get through all the data.
                    
        Arguments:
            records {list} -- list of all the detector count messages to be added to kinesis
            di_framework_client {DIFramework} -- Data Integration client to manage job status logging
        
        Keyword Arguments:
            batch_size {int} -- the size of each batch sent to kinesis in one put_records() call (default: {10})
            partition_key {str} --  (default: {"region"})
        """
        for batch in utils.chunks(records, batch_size):
            records_batch = [self.generate_kinesis_record(partition_key, record) for record in batch]
            self.write_records_to_kinesis(records_batch, di_framework_client)

    def generate_kinesis_record(self,partition_key, data):
        """Returns a Dict of with fields required by kinesis, encoding the data.
        
        Arguments:
            partition_key {str} -- key used by kinesis to determine which shard the data is written in.
            data {dict} -- Data to be encoded
        Returns:
            Dict
        """
        return {
            "PartitionKey": partition_key,
            "Data": json.dumps(data).encode('utf-8')
        }
    
    def write_records_to_kinesis(self,records,di_framework_client,retry=True):
        """Writes a batch of records into kinesis
        
        Arguments:
            records {list} -- list of Dicts that are ready to be added into kinesis
            di_framework_client {DIFramework} -- Data Integration client to manage job status logging
        
        Keyword Arguments:
            retry {bool} -- if this flag is true there will be one attempt to retry the failed records. (default: {True})
        """
        try:
            response = self.kinesis_client.put_records(Records=records, StreamName=self.stream_name)
            if(int(response["FailedRecordCount"])>0):
                error_message = f'{response["FailedRecordCount"]} out of {len(response["Records"])} records failed when being added to kinesis'
                log.error(error_message)
                failed_records = self.get_failed_records(records,response)
                di_framework_client.log_job_status(error_message)
                if(retry and len(failed_records) > 0):
                    time.sleep(2)
                    return self.write_records_to_kinesis(failed_records,di_framework_client,retry=False)
                else:
                    return response
            else:
                return response
        except Exception as e:
            log.error("An error occured when attempting to add records to kinesis.")
            log.error(e)
            di_framework_client.log_job_status(e)

    def get_failed_records(self, all_attempted_records, response):
        """Returns a list of all the records that failed because of the kinesis rate limit
        
        Arguments:
            all_attempted_records {list} -- the list of records that were originially attempted to be put in kinesis
            response {dict} -- the response from a kinesis put_records() call
        """
        failed_records = []
        for index, record in enumerate(response["Records"]):
            if "ErrorCode" in record and record["ErrorCode"] == "ProvisionedThroughputExceededException":
                failed_records.append(all_attempted_records[index])
        return failed_records