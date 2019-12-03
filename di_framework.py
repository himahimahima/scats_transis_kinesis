import psycopg2
import json

class DIFramework:
    """A wrapper class to allow for the creation, deletion and logging of jobs in the Data Integration Framework.

    Attributes:
        connection_details          (dict): Connection details for the database
        schema_name                  (str): Schema name of where the di framework tables are created.
        job_name                     (str): Name of the job being started
        __connection (psycopg2.connection): Connection to the database where the di framework is located

    """
    def __init__(self,config):
        self.connection_details = config["connection_details"]
        self.schema_name = config["schema_name"]
        self.job_name = config["job_name"]
    
    
    def start_db_connection(self):
        """Starts the database connection to where there DI framework tables are"""
        connection = psycopg2.connect(host=self.connection_details["host"]
                                 ,database=self.connection_details["database"]
                                     ,user=self.connection_details["user"]
                                 ,password=self.connection_details["password"])
        connection.autocommit = True
        return connection
    
    def close_db_connection(self):
        try:
            self.__connection.close()
        except AttributeError:
            pass
    
    def start_job(self):
        """Starts a job instance, return a response wich includes the job_id needed for closing and updating jobs"""
        self.__connection = self.start_db_connection()        
        sql_statement = f"SELECT {self.schema_name}.strt_job('{self.job_name}')"
        response = self.call_di_framework(sql_statement)
        response_json = json.loads(response[0])
        self.__active_job_id = self.get_value_from_response(response_json, "job_id")
        return response_json

    def get_value_from_response(self,response,key):
        """Returns the value from DI Framework response, given a key
        
        Arguments:
            response {json} -- response from the DI framework
            key {str} -- key required
        Returns:
            {str} -- the value for the given key in the response provided
        """
        key_value_pair = [x for x in response if x["key"] == key][0]
        value = key_value_pair["value"]
        return value

    def log_job_status(self,status_desc,job_id=None):
        """Updates the status of a job, returning True if sucessfully logged and False if failed
        
        Arguments:
            job_id {str} -- job id number
            status_desc {str} -- Short description of the status of the job
        """
        if not job_id:
            job_id = self.__active_job_id
        sql_statement = f"SELECT {self.schema_name}.log_job_stus('{self.job_name}', {job_id}, '{status_desc}')"
        response = self.call_di_framework(sql_statement)
        # The response from this in a malformed JSON string, so we will just look for the word success
        if 'success' in response[0]:
            return True
        else:
            return False

    def end_job(self,job_id=None):
        """Ends a job to denote a sucessfull completion of the job
        
        Arguments:
            job_id {str} -- job id number
        """
        if not job_id:
            job_id = self.__active_job_id        
        sql_statement = f"SELECT {self.schema_name}.end_job('{self.job_name}', {job_id})"
        response = self.call_di_framework(sql_statement)
        self.__connection.close()
        self.__active_job_id = None
        return json.loads(response[0])

    def error_job(self,error_message,job_id=None,job_status_cd="-1"):
        """Logs an error for a given job
        
        Arguments:
            job_id {str} -- job id number
            error_message {str} -- Short description of the error
        
        Keyword Arguments:
            job_status_cd {str} -- optional error code (default: {"-1"})
        """
        if not job_id:
            job_id = self.__active_job_id        
        sql_statement = f"SELECT {self.schema_name}.end_job('{self.job_name}', '{error_message}', {job_id}, {job_status_cd})"
        response = self.call_di_framework(sql_statement)
        self.__connection.close()
        self.__active_job_id = None
        return json.loads(response[0])
    
    def call_di_framework(self,sql_statement):
        """Returns the response from a call to a stored proc in the di framework
        
        Arguments:
            sql_statement {str} -- the select statement to be executed
        """
        cursor = self.__connection.cursor()
        cursor.execute(sql_statement)        
        response = cursor.fetchone()
        cursor.close()
        return response        
