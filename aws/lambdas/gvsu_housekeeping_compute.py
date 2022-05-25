import json
import os
import traceback
import boto3
import botocore
import datetime
from botocore.exceptions import ClientError
from botocore.config import Config
import gvsu_util as util
from boto3.dynamodb.conditions import Key, Attr

def lambda_handler(event, context):
    """ Lambda function to perform Ascend HouseKeeping Operations services. Part of Ascend Housekeeping service """
    try:
        util.logger.info('====================')
        eventTime =  datetime.datetime.strptime(event['time'], '%Y-%m-%dT%H:%M:%SZ') 
        formattedEventTime = eventTime.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        util.logger.info('Processing operations with request_ts < ' + formattedEventTime)

        operations_table: str = os.environ['ASCEND_HK_TABLE'].strip()
   
        housekeeping_compute_success: bool = process_api_request(formattedEventTime, operations_table)

        result: dict = {
            'housekeeping_compute_success': True,
            'description': f"Performing HouseKeeping operations"
        } if housekeeping_compute_success else {
            'housekeeping_compute_success': False,
            'description': f"Can not perform HouseKeeping operations"
        }

        util.logger.info("SUCCESS")
        return {
            'statusCode': 200,
            'headers': { 'Content-Security-Policy': 'default-src', 'X-XSS-Protection': '1; mode=block', 'Strict-Transport-Security': 'max-age=31536000' },
            'body': json.dumps(result)
        }
   
    except BaseException as ex:
        err_msg: str = f"[gvsu_housekeeping_compute] {ex.__class__.__name__}: {ex}"
        util.logger.error(traceback.format_exc())
        util.logger.info("Returning error '%s'", err_msg)
        util.logger.info("FAILED")
        return {
            'statusCode': 500,
            'body': json.dumps(err_msg)
        }


def process_api_request(eventTS: dict, operations_table: str) -> bool:

    try:

        rows = getRequestsToProcess(eventTS, operations_table)
        
        if len(rows) == 0:
            util.logger.info('No requested obtained to process at this time with  request_ts < ' + eventTS + ' in database: ' + operations_table)
            return True

        util.logger.info(str(len(rows)) + ' operations found to be processed in database: ' + operations_table)

        for thisRequest in rows:
            if thisRequest['operation_type'] == 'DELETE':
                try:
                    updateHKOperationStatus(thisRequest['operation_id'], 'PROCESSING')
                    invokeDeleteLambda(thisRequest)
                except BaseException as error:
                    util.logger.info('Going to update the operation to FAILED')
                    updateHKOperationStatus(thisRequest['operation_id'], 'FAILED', error)
                    pass
            else:
                raise Exception('Unknown operation_type : ' +  thisRequest['operation_type'])

    except BaseException as error:
        util.logger.info('Error trying to process housekeeping operation from database: ' + operations_table)
        raise error

    return True    

def getRequestsToProcess(eventTS: dict, operations_table: str):
    try:
        pendingRequests = []

        dynamodbResource = boto3.resource('dynamodb', config=Config(connect_timeout=5, read_timeout=10, retries={'max_attempts': 10}))
        
        table = dynamodbResource.Table(operations_table)
        
        response = table.scan(FilterExpression=Attr('request_ts').lt(eventTS) & Attr('status').ne('COMPLETED'),
                              ProjectionExpression='#st, request_ts, operation_id, table_name, job_database, data_location, operation_type',
                              ExpressionAttributeNames={"#st": "status"})

        while True:
            items = response.get('Items')
            if items != None:
                pendingRequests += items

            nextToken = response.get('LastEvaluatedKey')

            if nextToken == None:
                break
            else:
                response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'], 
                                      FilterExpression=Attr('request_ts').lt(eventTS) & Attr('status').ne('COMPLETED'),
                                      ProjectionExpression='#st, request_ts, operation_id, table_name, job_database, data_location, operation_type',
                                      ExpressionAttributeNames={"#st": "status"})
        
        return pendingRequests

    except BaseException as error:
        util.logger.info('Error trying to process housekeeping operation from database: ' + operations_table)
        raise error

    return True   

def updateHKOperationStatus(operationId: str, status: str, error_msg: str = ''): 
    operations_table: str = os.environ['ASCEND_HK_TABLE'].strip()

    try:
        dynamodbResource = boto3.resource('dynamodb', config=Config(connect_timeout=5, read_timeout=10, retries={'max_attempts': 10}))
        table = dynamodbResource.Table(operations_table)

        now = datetime.datetime.now()
        now_time_stamp = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        response = table.update_item(
            Key={
                "operation_id": operationId
            },
            UpdateExpression = "set #st = :s, start_ts = :c, retry_cnt = retry_cnt + :num, error_msg = :error ",
            ExpressionAttributeNames={
                "#st": "status"
            },
            ExpressionAttributeValues={
                ":s": status,
                ":c": now_time_stamp,
                ":num" : 1,
                ":error" : error_msg
            },
            ReturnValues="NONE"
        )
        
    except BaseException as error:
        util.logger.info('Error trying to update housekeeping operation: ' + operationId + ' in table: ' + operations_table)
        raise BaseException(error)

def invokeDeleteLambda(request):
    try:
        lambda_function: str = os.environ['DELETE_LAMBDA'].strip()

        payload = dict()
        payload['table_name']    = request['table_name']
        payload['job_database']  = request['job_database']
        payload['operation_id']  = request['operation_id']
        payload['data_location'] = request['data_location']
        payload_body = dict()
        payload_body["body"] = payload

        lambda_func: util.CLambda = util.CLambda()
        lambda_func.invoke_asynch(lambda_function, payload_body)
        util.logger.info("SUCCESS")
        
        return {
            'statusCode': 200,
            'headers': {'Content-Security-Policy': 'default-src', 'X-XSS-Protection': '1; mode=block', 'Strict-Transport-Security': 'max-age=7200'},
            'body': json.dumps("success")
        }
    except BaseException as error:
        util.logger.info('Error trying to invoke Delete Lambda for operation_id:' + request['operation_id'])
        raise BaseException(error)

