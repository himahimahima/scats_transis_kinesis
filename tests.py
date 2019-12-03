r"""these are all the tests of the transis to kinesis connector"""

import unittest
from unittest.mock import Mock, patch
from transis_consumer import TransisConsumer
from kinesis_producer import KinesisProducer
from transis_kinesis_connector import TransisKinesisConnector
import di_framework
import transis_response_models
import requests
import json
import logging

logger = logging.getLogger()
logger.level = logging.DEBUG

class TransisConsumerTests(unittest.TestCase):
    def setUp(self):
        self.simple_transis_response = b'<?xml version="1.0" encoding="UTF-8" standalone="yes"?><ns2:TransisResponse error="false" xmlns:ns2="http://model.transis.rta.nsw.gov.au/"><DetectorCountMessages><ns2:DetectorCountMessage Sid="2087" date="2019-10-03T15:43:00+10:00" reg="ROZ"><Detectors><Detector Did="21" count="0"/><Detector Did="20" count="0"/><Detector Did="18" count="0"/><Detector Did="19" count="0"/><Detector Did="1" count="0"/><Detector Did="7" count="0"/><Detector Did="12" count="0"/><Detector Did="6" count="1"/><Detector Did="13" count="0"/><Detector Did="8" count="0"/><Detector Did="11" count="0"/><Detector Did="9" count="0"/><Detector Did="10" count="0"/><Detector Did="14" count="0"/><Detector Did="15" count="0"/><Detector Did="17" count="0"/><Detector Did="16" count="0"/><Detector Did="24" count="0"/><Detector Did="2" count="0"/><Detector Did="3" count="0"/><Detector Did="23" count="0"/><Detector Did="5" count="0"/><Detector Did="22" count="0"/><Detector Did="4" count="0"/></Detectors></ns2:DetectorCountMessage></DetectorCountMessages></ns2:TransisResponse>\x00'
        self.multiple_xml_docs_transis_response = b'<?xml version="1.0" encoding="UTF-8" standalone="yes"?><ns2:TransisResponse error="false" xmlns:ns2="http://model.transis.rta.nsw.gov.au/"><DetectorCountMessages><ns2:DetectorCountMessage Sid="2087" date="2019-10-03T15:43:00+10:00" reg="ROZ"><Detectors><Detector Did="21" count="0"/><Detector Did="20" count="0"/><Detector Did="18" count="0"/><Detector Did="19" count="0"/><Detector Did="1" count="0"/><Detector Did="7" count="0"/><Detector Did="12" count="0"/><Detector Did="6" count="1"/><Detector Did="13" count="0"/><Detector Did="8" count="0"/><Detector Did="11" count="0"/><Detector Did="9" count="0"/><Detector Did="10" count="0"/><Detector Did="14" count="0"/><Detector Did="15" count="0"/><Detector Did="17" count="0"/><Detector Did="16" count="0"/><Detector Did="24" count="0"/><Detector Did="2" count="0"/><Detector Did="3" count="0"/><Detector Did="23" count="0"/><Detector Did="5" count="0"/><Detector Did="22" count="0"/><Detector Did="4" count="0"/></Detectors></ns2:DetectorCountMessage></DetectorCountMessages></ns2:TransisResponse>\x00<?xml version="1.0" encoding="UTF-8" standalone="yes"?><ns2:TransisResponse error="false" xmlns:ns2="http://model.transis.rta.nsw.gov.au/"><DetectorCountMessages><ns2:DetectorCountMessage Sid="2087" date="2019-10-03T15:43:00+10:00" reg="ROZ"><Detectors><Detector Did="21" count="0"/><Detector Did="20" count="0"/><Detector Did="18" count="0"/><Detector Did="19" count="0"/><Detector Did="1" count="0"/><Detector Did="7" count="0"/><Detector Did="12" count="0"/><Detector Did="6" count="1"/><Detector Did="13" count="0"/><Detector Did="8" count="0"/><Detector Did="11" count="0"/><Detector Did="9" count="0"/><Detector Did="10" count="0"/><Detector Did="14" count="0"/><Detector Did="15" count="0"/><Detector Did="17" count="0"/><Detector Did="16" count="0"/><Detector Did="24" count="0"/><Detector Did="2" count="0"/><Detector Did="3" count="0"/><Detector Did="23" count="0"/><Detector Did="5" count="0"/><Detector Did="22" count="0"/><Detector Did="4" count="0"/></Detectors></ns2:DetectorCountMessage></DetectorCountMessages></ns2:TransisResponse>\x00'
        with open("local_config.json","r") as file_handle: 
            self.env_variables = json.loads(file_handle.read())

        self.transis_consumer = TransisConsumer(self.env_variables["transis_config_prod"])
        self.mock_get_patcher = patch('transis_consumer.requests.get')
        self.mock_iter_content_patcher = patch('transis_consumer.requests.Response.iter_content')
        self.mock_get = self.mock_get_patcher.start()
        self.mock_iter_content = self.mock_iter_content_patcher.start()

    def tearDown(self):
        self.mock_get_patcher.stop()
        self.mock_iter_content_patcher.stop()

    def test_get_detector_counts_returns_transis_response_object(self):
        self.mock_iter_content.return_value = mock_iter_content(self.simple_transis_response)
        for response in self.transis_consumer.get_detector_counts():
            self.assertEqual(type(response),transis_response_models.TransisResponse)
    
    def test_get_detector_counts_handles_multiple_xml_docs(self):
        self.mock_iter_content.return_value = mock_iter_content(self.multiple_xml_docs_transis_response)
        responses = []
        for response in self.transis_consumer.get_detector_counts():
            responses.append(response)
        self.assertEqual(len(responses),2)
    
    def test_incorrect_authenication_raises_exception(self):
        transis_consumer = TransisConsumer({
            "hostname" :"163.189.13.221",
            "port"     :"2412",
            "username" :"bad_user",
            "password" :"bad_pass"
        })
        with self.assertRaises(requests.exceptions.HTTPError):
            list(transis_consumer.get_detector_counts())
    

class TransisResponseModelsTests(unittest.TestCase):
    def setUp(self):
        self.simple_transis_response_byte_string = b'<?xml version="1.0" encoding="UTF-8" standalone="yes"?><ns2:TransisResponse error="false" xmlns:ns2="http://model.transis.rta.nsw.gov.au/"><DetectorCountMessages><ns2:DetectorCountMessage Sid="2087" date="2019-10-03T15:43:00+10:00" reg="ROZ"><Detectors><Detector Did="21" count="5"/><Detector Did="20" count="6"/><Detector Did="18" count="12"/><Detector Did="19" count="0"/><Detector Did="1" count="0"/><Detector Did="7" count="0"/><Detector Did="12" count="0"/><Detector Did="6" count="1"/><Detector Did="13" count="0"/><Detector Did="8" count="0"/><Detector Did="11" count="0"/><Detector Did="9" count="0"/><Detector Did="10" count="0"/><Detector Did="14" count="0"/><Detector Did="15" count="0"/><Detector Did="17" count="0"/><Detector Did="16" count="0"/><Detector Did="24" count="0"/><Detector Did="2" count="0"/><Detector Did="3" count="0"/><Detector Did="23" count="0"/><Detector Did="5" count="0"/><Detector Did="22" count="0"/><Detector Did="4" count="0"/></Detectors></ns2:DetectorCountMessage></DetectorCountMessages></ns2:TransisResponse>'
        self.multi_site_transis_response_byte_string = b'<?xml version="1.0" encoding="UTF-8" standalone="yes"?><ns2:TransisResponse error="false" xmlns:ns2="http://model.transis.rta.nsw.gov.au/"><DetectorCountMessages><ns2:DetectorCountMessage Sid="2087" date="2019-10-03T15:43:00+10:00" reg="ROZ"><Detectors><Detector Did="21" count="0"/><Detector Did="20" count="0"/><Detector Did="18" count="0"/><Detector Did="19" count="0"/><Detector Did="1" count="0"/><Detector Did="7" count="0"/><Detector Did="12" count="0"/><Detector Did="6" count="1"/><Detector Did="13" count="0"/><Detector Did="8" count="0"/><Detector Did="11" count="0"/><Detector Did="9" count="0"/><Detector Did="10" count="0"/><Detector Did="14" count="0"/><Detector Did="15" count="0"/><Detector Did="17" count="0"/><Detector Did="16" count="0"/><Detector Did="24" count="0"/><Detector Did="2" count="0"/><Detector Did="3" count="0"/><Detector Did="23" count="0"/><Detector Did="5" count="0"/><Detector Did="22" count="0"/><Detector Did="4" count="0"/></Detectors></ns2:DetectorCountMessage><ns2:DetectorCountMessage Sid="8" date="2019-10-03T15:43:00+10:00" reg="ROZ"><Detectors><Detector Did="21" count="0"/><Detector Did="20" count="0"/><Detector Did="18" count="0"/><Detector Did="19" count="0"/><Detector Did="1" count="0"/><Detector Did="7" count="0"/><Detector Did="12" count="0"/><Detector Did="6" count="1"/><Detector Did="13" count="0"/><Detector Did="8" count="0"/><Detector Did="11" count="0"/><Detector Did="9" count="0"/><Detector Did="10" count="0"/><Detector Did="14" count="0"/><Detector Did="15" count="0"/><Detector Did="17" count="0"/><Detector Did="16" count="0"/><Detector Did="24" count="0"/><Detector Did="2" count="0"/><Detector Did="3" count="0"/><Detector Did="23" count="0"/><Detector Did="5" count="0"/><Detector Did="22" count="0"/><Detector Did="4" count="0"/></Detectors></ns2:DetectorCountMessage></DetectorCountMessages></ns2:TransisResponse>'
    
    def tearDown(self):
        pass

    def test_is_error_returns_true_for_error(self):
        transis_response = transis_response_models.TransisResponse(self.simple_transis_response_byte_string)
        self.assertEqual(transis_response.is_error(), False)

    def test_is_error_returns_false_for_no_error(self):
        errored_transis_response_byte_string = b'<?xml version="1.0" encoding="UTF-8" standalone="yes"?><ns2:TransisResponse error="true" xmlns:ns2="http://model.transis.rta.nsw.gov.au/"><DetectorCountMessages><ns2:DetectorCountMessage Sid="2087" date="2019-10-03T15:43:00+10:00" reg="ROZ"><Detectors><Detector Did="21" count="0"/><Detector Did="20" count="0"/><Detector Did="18" count="0"/><Detector Did="19" count="0"/><Detector Did="1" count="0"/><Detector Did="7" count="0"/><Detector Did="12" count="0"/><Detector Did="6" count="1"/><Detector Did="13" count="0"/><Detector Did="8" count="0"/><Detector Did="11" count="0"/><Detector Did="9" count="0"/><Detector Did="10" count="0"/><Detector Did="14" count="0"/><Detector Did="15" count="0"/><Detector Did="17" count="0"/><Detector Did="16" count="0"/><Detector Did="24" count="0"/><Detector Did="2" count="0"/><Detector Did="3" count="0"/><Detector Did="23" count="0"/><Detector Did="5" count="0"/><Detector Did="22" count="0"/><Detector Did="4" count="0"/></Detectors></ns2:DetectorCountMessage></DetectorCountMessages></ns2:TransisResponse>'
        transis_response = transis_response_models.TransisResponse(errored_transis_response_byte_string)
        self.assertEqual(transis_response.is_error(),True)
    
    def test_detector_count_messages_exist(self):
        transis_response = transis_response_models.TransisResponse(self.simple_transis_response_byte_string)
        self.assertEqual(type(transis_response.detector_count_messages),transis_response_models.DetectorCountMessages)

    def test_number_of_sites(self):
        transis_response = transis_response_models.TransisResponse(self.multi_site_transis_response_byte_string)
        get_num_sites = transis_response.detector_count_messages.get_num_sites()
        self.assertEqual(get_num_sites,2)
    
    def test_detector_count_message_to_dict(self):
        transis_response = transis_response_models.TransisResponse(self.simple_transis_response_byte_string)
        detector_message_dict = transis_response.detector_count_messages.detector_count_message_list[0].to_dict()
        expected_dict = {
            "collectionIntervalSecs": 300,
            "collectionendtimestamp_plus_3_mins": '2019-10-03T15:43:00+10:00',
            "detectorCounts": { 
                '1': '0',
                '10': '0',
                '11': '0',
                '12': '0',
                '13': '0',
                '14': '0',
                '15': '0',
                '16': '0',
                '17': '0',
                '18': '12',
                '19': '0',
                '2': '0',
                '20': '6',
                '21': '5',
                '22': '0',
                '23': '0',
                '24': '0',
                '3': '0',
                '4': '0',
                '5': '0',
                '6': '1',
                '7': '0',
                '8': '0',
                '9': '0'
             },
            "region": 'ROZ',
            "siteId": '2087'
        }
        self.assertEqual(detector_message_dict,expected_dict)


class KinesisProducerTests(unittest.TestCase):
    def setUp(self):
        pass
    
    def tearDown(self):
        pass

    def test_write_records_to_kinesis_reattempts_only_failed_records(self):
        """tests that only the records that kinesis says have failed are the the ones that will be re-pushed"""
        mocked_kinesis_client = Mock()
        mocked_kinesis_client.put_records = mock_put_records
        mocked_di_framework_client = Mock()
        records = [{"PartitionKey": "test", "Data": {"id": "1"}, "fail":True},
                   {"PartitionKey": "test", "Data": {"id": "2"}, "fail":False},
                   {"PartitionKey": "test", "Data": {"id": "3"}, "fail":True},
                   {"PartitionKey": "test", "Data": {"id": "4"}, "fail":False}]


        kinesis_producer = KinesisProducer("region_name","stream_name",mocked_kinesis_client)
        res = kinesis_producer.write_records_to_kinesis(records,mocked_di_framework_client)
        number_of_expected_failed_records = 2
        expected_res = {
            'FailedRecordCount': number_of_expected_failed_records,
            'Records': [{'ErrorCode': 'ProvisionedThroughputExceededException','ErrorMessage': 'this is a mock error message'}]*number_of_expected_failed_records,
            'EncryptionType': 'NONE'
        }
        self.assertEqual(res,expected_res)


def mock_iter_content(byte_string,chunk_size=1):
    """A mock of the requests.Response.iter_content used in transis_consumer to read the stream in get_detector_counts()"""
    bytes_list = [byte_string[i:i+1] for i in range(len(byte_string))]
    for b in bytes_list:
        yield b

def mock_put_records(Records,StreamName):
    """Mock method to simulate the behaviour of the boto3.client.Kinesis put_records() method

    Note:
        There is one extra key "fail" which will determine if a record fails or not
    
    Arguments:
        {dict} -- A record similar to what happens when a 
    """
    return {
        'FailedRecordCount': len([x for x in Records if x["fail"]]),
        'Records': [{'ErrorCode': 'ProvisionedThroughputExceededException','ErrorMessage': 'this is a mock error message'}  if x["fail"] else {'SequenceNumber': 123,'ShardId': 'MockShardId'} for x in Records],
        'EncryptionType': 'NONE'
    }

if __name__ == '__main__':    
    unittest.main(verbosity=2)
               