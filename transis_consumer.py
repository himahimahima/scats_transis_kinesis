r"""
transis_consumer.py is responsible for managing the connection with and recieving data from transis
"""

import requests
import logging
from transis_response_models import TransisResponse
log = logging.getLogger(__name__)

class TransisConsumer:
    """Represents the connector to Transis. Can create a connection and request streams of data"""
    
    def __init__(self,connection_details,stream_timeout=20*60, max_transis_reconnects=3):
        self.connection_details = connection_details
        self.stream_timeout = stream_timeout
        self.set_max_transis_reconnects(max_transis_reconnects)

        domain  = "http://{hostname}:{port}/transis".format(hostname=self.connection_details["hostname"],port=self.connection_details["port"])
        # self.endpoints = {
        #     "getFromDate": domain+"/rest/getFromDate?startDate={start_date}&types={types}",
        #     "getWithinDates": domain+"/rest/getWithinDates?startDate={start_date}&endDate={end_date}&types={types}",
        #     "getCurrentTopology": domain+"/rest/getCurrentTopology",
        #     "getTopologyChangesFromDate": domain+"/rest/getTopologyChangesFromDate?date={from_date}",
        #     "getAllOpenTIRF": domain+"/rest/getAllOpenTIRF",
        #     "getClosedTIRFFromDate": domain+"/rest/getClosedTIRFFromDate?date={from_date}",
        #     "getAllVMS": domain+"/rest/getAllVMS",
        #     "streamDetectorCount": domain+"/pushservice?types=DetectorCount"
        # }

        self.endpoints = {
            "getFromDate": f"{domain}/rest/getFromDate",
            "getWithinDates": f"{domain}/rest/getWithinDates",
            "getCurrentTopology": f"{domain}/rest/getCurrentTopology",
            "getTopologyChangesFromDate": f"{domain}/rest/getTopologyChangesFromDate",
            "getAllOpenTIRF": f"{domain}/rest/getAllOpenTIRF",
            "getClosedTIRFFromDate": f"{domain}/rest/getClosedTIRFFromDate",
            "getAllVMS": f"{domain}/rest/getAllVMS",
            "streamDetectorCount": f"{domain}/pushservice?types=DetectorCount"
        }

    def set_max_transis_reconnects(self,max_reconnects):
        """Allows for manual override of the default __max_reconnects value"""
        self.__max_reconnects = max_reconnects
        self.__reconnect_attempts_remaining = max_reconnects

    def __reset_connection_attempt_counts(self):
        self.__reconnect_attempts_remaining = self.__max_reconnects          

    def get_transis_detector_count_stream(self):
        """Returns the detector count stream from Transis 
        
        Create a GET request to transis for DetectorCount and return the response
        Transis has tendancy to stop sending after some time but does not close the connection, so we have a timeout of self.stream_timeout seconds.
        
        The bytestream is a continuous collection of XML records seperated by a null byte
        
        Returns:
            response (requests.Response): The reponse that can be used to obtain the DetectorCount stream data
        """
        endpoint = "http://{hostname}:{port}/transis/pushservice?types=DetectorCount".format(hostname=self.connection_details["hostname"],port=self.connection_details["port"])
        log.info(f"Making GET request to {endpoint}")
        response = self.session.get(endpoint,stream=True,timeout=self.stream_timeout, headers={
            'Content-type':'text/xml;charset="utf-8"', 
            'Authorization':'Basic',
            'Connection': 'close'
            })
        response.raise_for_status()
        return response
    
    def get_transis_topology(self):
        endpoint = "http://{hostname}:{port}/transis/rest/getCurrentTopology".format(hostname=self.connection_details["hostname"],port=self.connection_details["port"])
        log.info(f"Making GET request to {endpoint}")
        response = self.session.get(endpoint,timeout=self.stream_timeout, headers={
            'Content-type':'text/xml;charset="utf-8"', 
            'Authorization':'Basic',
            'Connection': 'close'
            })
        response.raise_for_status()
        body = response.content.rstrip(b"\x00")
        return TransisResponse(body)

    def __get_transis_responses(self, response):
        """[summary]
        
        Arguments:
            endpoint {[type]} -- [description]
        """
        responses = response.content.split(b"\x00")
        return [TransisResponse(r) for r in responses if r != b""]  

    def __get_http_response(self,endpoint,stream,**kwarg):
        """Returns the requests.Response object from a call to transis
        
        Arguments:
            endpoint {str} -- full transis http/s endpoint with paramaters
            stream {bool} -- defines if the expected response is a stream or not
        
        Returns:
            response (requests.Response): used to inspect the data from transis
        """
        try:
            self.session
        except AttributeError:
            self.start_transis_http_session()
        url = self.endpoints[endpoint]
        log.info(f"Making GET request to {url}")
        response = self.session.get(url=url,params=kwarg, stream=stream,timeout=self.stream_timeout, headers= {
            'Content-type':'text/xml;charset="utf-8"', 
            'Authorization':'Basic',
            'Connection': 'close'
        })
        response.raise_for_status()
        return response

    def get_detector_counts(self):
        """Generater to yield all the detector count messages, will try to reconnect to transis if there is no data recieved before the timeout is over.
        
        Note:
            Transis denotes the end of the xml with a null byte -- b'\x00'
        Yields:
            {transis_response_models.TransisResponse} -- Transis responses that have a a detector count messages
        """        
        byte_string = b""
        stream = self.__get_http_response("streamDetectorCount",stream=True)
        try:
            log.info("Waiting for detector count stream to recieve data, this may take around 10 minutes.")
            for chunk in stream.iter_content(chunk_size=1):
                if chunk and chunk!=b'\x00': # null byte denotes end of xml document.
                    byte_string+=chunk
                elif chunk==b'\x00':
                    transis_response_byte_string_list = byte_string.split(b'\x00')
                    for transis_response_byte_string in transis_response_byte_string_list:
                        transis_response = TransisResponse(transis_response_byte_string)
                        err_msg = transis_response.is_error()
                        if(err_msg):
                            raise Exception(err_msg)
                        elif(transis_response.detector_count_messages):
                            yield TransisResponse(transis_response_byte_string)
                    byte_string = b""
                    self.__reset_connection_attempt_counts()
        except requests.exceptions.ConnectionError as e:
            if self.__reconnect_attempts_remaining > 0:
                log.error(f"Transis has not responded for {self.stream_timeout} seconds, will attempt to reconnect {self.__reconnect_attempts_remaining} more time(s)")
                self.__reconnect_attempts_remaining -= 1
                for r in self.get_detector_counts():
                    yield r
            else:
                raise Exception(f"{self.__max_reconnects} attempts to reconnect to transis were made without success.")
        except Exception as e:
            log.error(f"An error occured when processing the transis detector counts stream:  {e}")
            raise e
    
    def get_current_topology(self):
        res = self.__get_http_response("getCurrentTopology",False)
        return self.__get_transis_responses(res)[0]

    def get_topology_changes_from(self, from_date):
        """Returns all the topology change from the given date
        
        Arguments:
            from_date {str} -- get change from this date onwards e.g 2019-10-20T21:43:32.000+11:00
        """
        res = self.__get_http_response("getTopologyChangesFromDate",stream=False,date=from_date)
        return self.__get_transis_responses(res)[0]

    def get_data_from(self,types,from_date):
        """Returns the data of the requested type from the given date
        
        Arguments:
            from_date {str} -- get change from this date onwards e.g 2019-10-20T21:43:32.000+11:00
        """        
        res = self.__get_http_response("getFromDate",stream=False,startDate=from_date,types=types)
        return self.__get_transis_responses(res)[0]        

    def get_strategic_monitor_from(self, from_date):
        """Returns all the StrategicMonitor records from the given date
        
        Arguments:
            from_date {str} -- get change from this date onwards e.g 2019-10-20T21:43:32.000+11:00
        """
        res = self.__get_http_response("getFromDate",stream=False,startDate=from_date,types="StrategicMonitor")
        return self.__get_transis_responses(res)[0]

    def get_motorway_from(self, from_date):
        """Returns all the Motorway data from the given date
        
        Arguments:
            from_date {str} -- get change from this date onwards e.g 2019-10-20T21:43:32.000+11:00
        """
        res = self.__get_http_response("getFromDate",stream=False,startDate=from_date,types="Motorway")
        return self.__get_transis_responses(res)[0]

    def get_site_alarm_from(self, from_date):
        """Returns all the Motorway data from the given date
        
        Arguments:
            from_date {str} -- get change from this date onwards e.g 2019-10-20T21:43:32.000+11:00
            domain+"/rest/getFromDate?startDate={start_date}&types={types}",
        """
        res = self.__get_http_response("getFromDate",stream=False,startDate=from_date,types="SiteAlarm")
        return self.__get_transis_responses(res)[0]

    def get_all_open_tirf(self):
        """Returns all the current open Traffic Interuption Request Form (TIRF) incidents at the time of the request.        
        """
        res = self.__get_http_response("getAllOpenTIRF",stream=False)
        return self.__get_transis_responses(res)[0]

    def get_all_closed_tirf(self,from_date):
        """Returns all the closed Traffic Interuption Request Form (TIRF) as of as of from_date
        
        Arguments:
            from_date {str} -- get change from this date onwards e.g 2019-10-20T21:43:32.000+11:00
        """
        res = self.__get_http_response("getClosedTIRFFromDate",stream=False,date=from_date)
        return self.__get_transis_responses(res)[0]

    def get_all_vms(self):
        """Returns a transis response with the current Variable Messaging Sign(VMS) data
        
        Returns:
            {transis_response_models.TransisResponse} -- The response object that can be inpected for the data.
        """
        res = self.__get_http_response("getAllVMS",stream=False)
        return self.__get_transis_responses(res)[0]


    def start_transis_http_session(self):
        """Starts an authenticated HTTP session with Transis"""
        self.session = requests.Session()
        self.session.auth=(self.connection_details["username"], self.connection_details["password"])
