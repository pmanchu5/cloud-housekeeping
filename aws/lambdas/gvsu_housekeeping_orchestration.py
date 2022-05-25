import json
import os
import traceback
import boto3
import datetime
from botocore.exceptions import ClientError
from botocore.config import Config
from utils import gvsu_util as util
import botocore
import uuid

def lambda_handler(event, context):
    """ Lambda function to orchestrate Ascend HouseKeeping Metadata services. Part of Ascend Housekeeping micro service """
    
    try:
        util.logger.info('====================')
        util.log_dict_debug("event: %s", event)

        body = event['body']
        if isinstance(body, str):  body = json.loads(body)
        util.log_dict_info('event body: %s', body)

        operations_table: str = os.environ['ASCEND_HK_TABLE'].strip()
        
        hkOperation = body.get("hk_operation")
        tableName = body.get("table_name")
        tableDatabase = body.get("job_database")
        runInstance = body.get("run_instance")
        requestTimestamp = body.get("request_ts")

        if hkOperation == '' or hkOperation == None or tableName == '' or tableDatabase == '' or requestTimestamp == '':
            paramValues = 'hk_operation:<' + str(hkOperation) + '> table_name:<' + str(tableName) + '> job_database:<' + str(tableDatabase) + '> request_ts:<' + str(requestTimestamp) + '> '
            raise RuntimeError ('Required parameters missing please review the request. '  + paramValues)

        if hkOperation == 'DELETE':
            pass
        else:
            message: str = f"Requested operation: " + hkOperation + " not supported"
            raise RuntimeError(message)

        housekeeping_orchestration_success: bool = process_api_request(body, operations_table)

        result: dict = {
            'housekeeping_orchestration_success': True,
            'description': f"Performing HouseKeeping operation for the given table '{tableDatabase}'.'{tableName}'"
        } if housekeeping_orchestration_success else {
            'housekeeping_orchestration_success': False,
            'description': f"Can not perform HouseKeeping operation for the given table '{tableDatabase}'.'{tableName}'"
        }

        util.logger.info("SUCCESS")
        return {
            'statusCode': 200,
            'headers': { 'Content-Security-Policy': 'default-src', 'X-XSS-Protection': '1; mode=block', 'Strict-Transport-Security': 'max-age=31536000' },
            'body': json.dumps(result)
        }

    except BaseException as ex:
        err_msg: str = f"[gvsu_housekeeping_orchestration] {ex.__class__.__name__}: {ex}"
        util.logger.error(traceback.format_exc())
        util.logger.info("Returning error '%s'", err_msg)
        util.logger.info("FAILED")
        return {
            'statusCode': 500,
            'body': json.dumps(err_msg)
        }

def process_api_request(request: dict, operations_table: str) -> bool:
    util.logger.debug('output content: %s', request)

    now = datetime.datetime.now()
    request_time_stamp = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    glueInfo = getGlueInfo(request['job_database'], request['table_name'])

    if glueInfo == None:
        message: str = f"Table(or View) [" + request['job_database'] + "." + request['table_name'] + "] cannot be found. "
        raise RuntimeError(message)

    if glueInfo['TableType'] == 'EXTERNAL_TABLE':
        tableType = 'TABLE'
        s3location = glueInfo['StorageDescriptor']['Location']
    elif glueInfo['TableType'] == 'VIRTUAL_VIEW':
        tableType = 'VIEW'
        s3location = ''
    else:
        message: str = f"Invalid table type : " + glueInfo['TableType'] + " (expected EXTERNAL_TABLE or VIRTUAL_VIEW)"
        raise RuntimeError(message)
    
    item = {
        'operation_id':   str(uuid.uuid1()),
        'table_name':     request['table_name'],
        'job_database':   request['job_database'],
        'table_type':     tableType,
        'data_location':  s3location,

        'operation_type': request['hk_operation'],
        'run_instance':   request['run_instance'],
        
        'status':         'REQUESTED',
        'requested_on':   request_time_stamp,
        'request_ts':     request['request_ts'],
        'start_ts':       '',
        'complete_ts':    '',
        'retry_cnt':      0,
        'error_msg':      '' 
    }

    try:
        dynamodbResource = boto3.resource('dynamodb', config=Config(connect_timeout=5, read_timeout=10, retries={'max_attempts': 10}))
        table = dynamodbResource.Table(operations_table)

        response = table.put_item(Item = item)

    except BaseException as error:
        util.logger.info('Error trying to insert new housekeeping item in database: ' + operations_table)
        util.logger.info(json.dumps(item, indent=4, sort_keys=True))
        raise RuntimeError(error)

    return True    

def getGlueInfo(database: str, tableName: str):
    glue_client = boto3.client('glue', config=Config(connect_timeout=5, read_timeout=10, retries={'max_attempts': 10}))

    try:
        response = glue_client.get_table(DatabaseName = database, Name = tableName)
        return response['Table']
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'EntityNotFoundException':
            return None
        else:
            util.logger.info('Error trying to find location for table:' + tableName + ' in database:' + database)
            raise RuntimeError(error)
    
