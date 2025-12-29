"""
Asynchronous Excalibur API Logger
Logs API calls to database without blocking the main application
Uses threading and queue for parallel execution
"""

from dotenv import load_dotenv
load_dotenv()

import os
import json
import threading
import queue
from datetime import datetime
from sqlalchemy import create_engine, text

class ExcaliburAPILogger:
    """
    Non-blocking logger for Excalibur API calls
    Uses a background thread and queue to prevent slowing down the main application
    """
    
    def __init__(self):
        self.database_uri = os.getenv("DATABASE_URI")
        self.engine = create_engine(self.database_uri, pool_pre_ping=True)
        
        # Set running flag BEFORE starting thread
        self.is_running = True
        
        # Queue for log entries
        self.log_queue = queue.Queue()
        
        # Background thread for processing logs
        self.worker_thread = threading.Thread(target=self._process_logs, daemon=True)
        self.worker_thread.start()
        
        print("ExcaliburAPILogger: Background logging thread started")
    
    def log_api_call(self, log_data):
        """
        Add an API call to the logging queue (non-blocking)
        
        Parameters:
        -----------
        log_data : dict
            Dictionary containing log information
            
        Example:
        --------
        logger.log_api_call({
            'api_endpoint': '/api/import/uploadfile',
            'http_method': 'POST',
            'client_id': '823842000801419',
            'matter_id': '000207',
            'phone_number': '27828492746',
            'http_status_code': 200,
            'response_time': 1234,
            'is_success': True,
            'file_name': 'document.pdf',
            'file_type': 'PDF',
            'document_type': '81'
        })
        """
        # Add timestamp if not provided
        if 'created_at' not in log_data:
            log_data['created_at'] = datetime.now()
        
        # Put in queue (non-blocking)
        self.log_queue.put(log_data)
    
    def _process_logs(self):
        """
        Background worker thread that processes the log queue
        Runs continuously in the background
        """
        while self.is_running:
            try:
                # Get log entry from queue (with timeout to allow checking is_running)
                log_data = self.log_queue.get(timeout=1)
                
                # Insert into database
                self._insert_log(log_data)
                
                # Mark task as done
                self.log_queue.task_done()
                
            except queue.Empty:
                # Queue is empty, continue loop
                continue
            except Exception as e:
                print(f"ExcaliburAPILogger: Error processing log: {e}")
    
    def _insert_log(self, log_data):
        """
        Insert log entry into [Bot].[dbo].[APICall] table
        Uses lookup tables for foreign keys: HttpMethod, HttpStatusCode, ErrorSource, Environment, FileType
        """
        try:
            with self.engine.connect() as conn:
                # Get foreign key IDs from lookup tables
                http_method_id = self._get_http_method_id(conn, log_data.get('http_method', 'POST'))
                http_status_code_id = self._get_http_status_code_id(conn, log_data.get('http_status_code'))
                error_source_id = self._get_error_source_id(conn, log_data.get('error_source'))
                environment_id = self._get_environment_id(conn, log_data.get('environment', 'Dev'))
                file_type_id = self._get_file_type_id(conn, log_data.get('file_type'))
                
                # Use defaults for required fields if NULL
                # ErrorSourceID: 5 = 'N/A' (for successful calls with no error)
                if error_source_id is None:
                    error_source_id = 5
                
                # FileTypeID: 49 = 'N/A' (for API calls with no file)
                if file_type_id is None:
                    file_type_id = 49
                
                sql = text("""
                    INSERT INTO [Bot].[dbo].[APICall] (
                        Endpoint,
                        HttpMethodID,
                        ClientPhoneNumber,
                        DebtorPhoneNumber,
                        HttpStatusCodeID,
                        ResponseTime,
                        IsSuccess,
                        ErrorSourceID,
                        ExceptionMessage,
                        RetryCount,
                        EnvironmentID,
                        MatterID,
                        FileName,
                        FileTypeID,
                        DateCreated
                    ) VALUES (
                        :endpoint,
                        :http_method_id,
                        :client_phone_number,
                        :debtor_phone_number,
                        :http_status_code_id,
                        :response_time,
                        :is_success,
                        :error_source_id,
                        :exception_message,
                        :retry_count,
                        :environment_id,
                        :matter_id,
                        :file_name,
                        :file_type_id,
                        :date_created
                    )
                """)
                
                # Prepare parameters
                params = {
                    'endpoint': log_data.get('api_endpoint'),
                    'http_method_id': http_method_id,
                    'client_phone_number': log_data.get('client_id') or log_data.get('client_phone_number'),
                    'debtor_phone_number': log_data.get('phone_number') or log_data.get('debtor_phone_number'),
                    'http_status_code_id': http_status_code_id,
                    'response_time': log_data.get('response_time'),
                    'is_success': 1 if log_data.get('is_success') else 0,
                    'error_source_id': error_source_id,
                    'exception_message': log_data.get('exception_message', '')[:500] if log_data.get('exception_message') else None,  # First 500 chars
                    'retry_count': log_data.get('retry_count', 0),
                    'environment_id': environment_id,
                    'matter_id': log_data.get('matter_id'),
                    'file_name': log_data.get('file_name'),
                    'file_type_id': file_type_id,
                    'date_created': log_data.get('created_at', datetime.now())
                }
                
                conn.execute(sql, params)
                conn.commit()
                
        except Exception as e:
            print(f"ExcaliburAPILogger: Database insert error: {e}")
    
    def _get_http_method_id(self, conn, http_method):
        """Get HttpMethodID from lookup table (POST=1, GET=2, PUT=3, DELETE=4)"""
        if not http_method:
            return None
        try:
            result = conn.execute(text("SELECT HttpMethodID FROM [Bot].[dbo].[HttpMethod] WHERE HttpMethodName = :method"), {'method': http_method}).fetchone()
            if result:
                return result[0]
            # If not found, log a warning (table should be pre-populated)
            print(f"ExcaliburAPILogger: Warning - HttpMethod '{http_method}' not found in lookup table")
            return None
        except Exception as e:
            print(f"ExcaliburAPILogger: Error getting HttpMethodID for '{http_method}': {e}")
            return None
    
    def _get_http_status_code_id(self, conn, status_code):
        """
        Get HttpStatusCodeID from lookup table
        NOTE: The ID IS the status code itself (200, 404, 500, etc.)
        Just return the status code directly, no lookup needed
        """
        if not status_code:
            return None
        try:
            # The HttpStatusCodeID is the actual HTTP status code
            return int(status_code)
        except Exception as e:
            print(f"ExcaliburAPILogger: Error converting status code '{status_code}': {e}")
            return None
    
    def _get_error_source_id(self, conn, error_source):
        """Get ErrorSourceID from lookup table, auto-insert if not exists"""
        if not error_source:
            return None
        try:
            result = conn.execute(text("SELECT ErrorSourceID FROM [Bot].[dbo].[ErrorSource] WHERE ErrorSourceName = :source"), {'source': error_source}).fetchone()
            if result:
                return result[0]
            # Auto-insert if doesn't exist
            conn.execute(text("INSERT INTO [Bot].[dbo].[ErrorSource] (ErrorSourceName) VALUES (:source)"), {'source': error_source})
            conn.commit()
            result = conn.execute(text("SELECT ErrorSourceID FROM [Bot].[dbo].[ErrorSource] WHERE ErrorSourceName = :source"), {'source': error_source}).fetchone()
            return result[0] if result else None
        except Exception as e:
            print(f"ExcaliburAPILogger: Error getting ErrorSourceID for '{error_source}': {e}")
            return None
    
    def _get_environment_id(self, conn, environment):
        """Get EnvironmentID from lookup table"""
        if not environment:
            environment = 'Dev'
        try:
            result = conn.execute(text("SELECT EnvironmentID FROM [Bot].[dbo].[Environment] WHERE EnvironmentName = :name"), {'name': environment}).fetchone()
            if result:
                return result[0]
            # Also try common variations
            if environment == 'Dev':
                result = conn.execute(text("SELECT EnvironmentID FROM [Bot].[dbo].[Environment] WHERE EnvironmentName = 'Development'")).fetchone()
                if result:
                    return result[0]
            # If not found, log a warning
            print(f"ExcaliburAPILogger: Warning - Environment '{environment}' not found in lookup table")
            return None
        except Exception as e:
            print(f"ExcaliburAPILogger: Error getting EnvironmentID for '{environment}': {e}")
            return None
    
    def _get_file_type_id(self, conn, file_type):
        """Get FileTypeID from lookup table, auto-insert if not exists"""
        if not file_type:
            return None
        try:
            result = conn.execute(text("SELECT FileTypeID FROM [Bot].[dbo].[FileType] WHERE FileTypeName = :type"), {'type': file_type}).fetchone()
            if result:
                return result[0]
            # Auto-insert if doesn't exist
            conn.execute(text("INSERT INTO [Bot].[dbo].[FileType] (FileTypeName) VALUES (:type)"), {'type': file_type})
            conn.commit()
            result = conn.execute(text("SELECT FileTypeID FROM [Bot].[dbo].[FileType] WHERE FileTypeName = :type"), {'type': file_type}).fetchone()
            return result[0] if result else None
        except Exception as e:
            print(f"ExcaliburAPILogger: Error getting FileTypeID for '{file_type}': {e}")
            return None
    
    def shutdown(self):
        """
        Gracefully shutdown the logger
        Waits for queue to be empty before stopping
        """
        print("ExcaliburAPILogger: Shutting down...")
        self.log_queue.join()  # Wait for queue to be processed
        self.is_running = False
        self.worker_thread.join(timeout=5)
        print("ExcaliburAPILogger: Shutdown complete")


# ============================================================================
# GLOBAL LOGGER INSTANCE (Singleton)
# ============================================================================

_logger_instance = None

def get_api_logger():
    """
    Get the global API logger instance (creates if doesn't exist)
    """
    global _logger_instance
    if _logger_instance is None:
        _logger_instance = ExcaliburAPILogger()
    return _logger_instance


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def log_excalibur_request(
    api_endpoint,
    client_id,
    matter_id=None,
    phone_number=None,
    request_payload=None,
    file_name=None,
    file_type=None,
    document_type=None,
    session_id=None,
    **kwargs
):
    """
    Log an Excalibur API request (call before making request)
    Returns a request_id that can be used to update the log with response
    """
    logger = get_api_logger()
    
    log_data = {
        'api_endpoint': api_endpoint,
        'http_method': kwargs.get('http_method', 'POST'),
        'client_id': client_id,
        'matter_id': matter_id,
        'phone_number': phone_number,
        'session_id': session_id,
        'request_start_time': datetime.now(),
        'file_uploaded': file_name is not None,
        'file_name': file_name,
        'file_type': file_type,
        'document_type': document_type,
    }
    
    # Add request payload if provided (optionally truncate large payloads)
    if request_payload:
        if isinstance(request_payload, dict):
            request_payload = json.dumps(request_payload)
        log_data['request_payload_size'] = len(request_payload)
        # Optionally truncate very large payloads to save space
        if len(request_payload) > 100000:  # 100KB
            log_data['request_payload'] = request_payload[:100000] + '... [truncated]'
        else:
            log_data['request_payload'] = request_payload
    
    # Add any additional kwargs
    log_data.update(kwargs)
    
    logger.log_api_call(log_data)


def log_excalibur_response(
    api_endpoint,
    client_id,
    http_status_code,
    response_time,
    response_payload=None,
    is_success=None,
    exception_thrown=False,
    exception_message=None,
    error_source=None,
    ssl_verified=True,
    retry_count=0,
    **kwargs
):
    """
    Log an Excalibur API response (call after receiving response)
    """
    logger = get_api_logger()
    
    # Extract response message from payload if it's JSON
    response_message = None
    if response_payload:
        try:
            if isinstance(response_payload, str):
                response_json = json.loads(response_payload)
            else:
                response_json = response_payload
            
            # Try to extract message from common locations
            if 'EnvelopeBody' in response_json and len(response_json['EnvelopeBody']) > 0:
                response_message = response_json['EnvelopeBody'][0].get('ResultMessage')
            elif 'EnvelopeFooter' in response_json:
                response_message = response_json['EnvelopeFooter'].get('ResponseMessage')
                exception_thrown = response_json['EnvelopeFooter'].get('ExceptionThrown', False)
                exception_message = response_json['EnvelopeFooter'].get('ExceptionMessage')
        except:
            pass
    
    # Determine success if not explicitly provided
    if is_success is None:
        is_success = http_status_code == 200 and not exception_thrown
    
    log_data = {
        'api_endpoint': api_endpoint,
        'client_id': client_id,
        'http_status_code': http_status_code,
        'response_time': response_time,
        'response_message': response_message,
        'is_success': is_success,
        'exception_thrown': exception_thrown,
        'exception_message': exception_message,
        'error_source': error_source,
        'ssl_verified': ssl_verified,
        'retry_count': retry_count,
        'request_end_time': datetime.now(),
    }
    
    # Add response payload
    if response_payload:
        if isinstance(response_payload, dict):
            response_payload = json.dumps(response_payload)
        log_data['response_payload'] = response_payload
    
    # Add any additional kwargs
    log_data.update(kwargs)
    
    logger.log_api_call(log_data)
