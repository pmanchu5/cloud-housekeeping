import json
import os
import traceback
import boto3
import datetime
import time
from urllib.parse import urlparse
from botocore.exceptions import ClientError
from botocore.config import Config
from utils import gvsu_util as util
import botocore

def lambda_handler(event, context):
    """ Lambda function to delete an Athena table and its underlying data in S3. Part of Ascend Housekeeping micro service """

    try:
        util.logger.info('====================')
        util.log_dict_debug("event: %s", event)

        body = event['body']
        if isinstance(body, str):  body = json.loads(body)
        util.log_dict_info('event body: %s', body)

        operations_table: str = os.environ['ASCEND_HK_TABLE'].strip()
        
        tableName = body.get("table_name")
        tableDatabase = body.get("job_database")

        if tableName == '' or tableDatabase == '':
            paramValues = ' table_name:<' + str(tableName) + '> job_database:<'
            raise RuntimeError ('Required parameters missing please review the request. '  + paramValues)

        housekeeping_delete_success: bool = process_api_request(body, operations_table)

        result: dict = {
            'housekeeping_delete_success': True,
            'description': f"Performing HouseKeeping DELETE for table '{tableDatabase}'.'{tableName}'"
        } if housekeeping_delete_success else {
            'housekeeping_delete_success': False,
            'description': f"Can not perform HouseKeeping DELETE for the given table '{tableDatabase}'.'{tableName}'"
        }

        util.logger.info("SUCCESS")
        return {
            'statusCode': 200,
            'headers': { 'Content-Security-Policy': 'default-src', 'X-XSS-Protection': '1; mode=block', 'Strict-Transport-Security': 'max-age=31536000' },
            'body': json.dumps(result)
        }

    except BaseException as ex:
        err_msg: str = f"[gvsu_housekeeping_delete] {ex.__class__.__name__}: {ex}"
        util.logger.error(traceback.format_exc())
        util.logger.info("Returning error '%s'", err_msg)
        util.logger.info("FAILED")
        return {
            'statusCode': 500,
            'body': json.dumps(err_msg)
        }

def process_api_request(request: dict, operations_table: str) -> bool:
    util.logger.debug('output content: %s', request)

    database = request['job_database'].lower()
    tableName = request['table_name'].lower()
    s3location = request.get('data_location','')

    try:
        glueInfo = getGlueInfo(database, tableName)

        if glueInfo == None:
            util.logger.info("Table(or View) [" + database + "." + tableName + "] cannot be found. ")
            deleteS3Objects(s3location)
        else:
            if glueInfo['TableType'] == 'EXTERNAL_TABLE':
                if s3location == '' :
                    s3location = glueInfo['StorageDescriptor']['Location']
                deleteAthenaArtifact(database, tableName, 'TABLE')
                deleteS3Objects(s3location)
            elif glueInfo['TableType'] == 'VIRTUAL_VIEW':
                s3location = ''
                deleteAthenaArtifact(database, tableName, 'VIEW')
            else:
                message: str = f"Invalid table type : " + glueInfo['TableType'] + " (expected EXTERNAL_TABLE or VIRTUAL_VIEW)"
                raise RuntimeError(message)
        
        operationId = request.get('operation_id')
        if operationId == None or operationId == '':
            util.logger.info('No operation_id provided to update the HK operation status')
        else:
            updateHKOperationStatus(operationId)

   
    except BaseException as error:
        util.logger.info('Error trying to delete ' + database + "." + tableName)
        raise RuntimeError(error)

    return True    

def deleteAthenaArtifact(database: str, tableName: str, artifactType: str):
    sleepTime = 1

    athena_client = boto3.client('athena', config=Config(connect_timeout=5, read_timeout=10, retries={'max_attempts': 10}))

    query = ' DROP ' + artifactType + ' IF EXISTS ' + tableName

    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        WorkGroup='ascend-housekeeping'
    )
    
    query_execution_object = response
    query_execution_id = response['QueryExecutionId']

    while True :
        query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        query_execution_status = query_status['QueryExecution']['Status']['State']
        
        if query_execution_status == 'SUCCEEDED':
            util.logger.info("Delete table/View:" + query_execution_status)
            break

        if query_execution_status == 'FAILED':
            raise Exception("Delete table/View:" + query_execution_status + " - " + query_status['QueryExecution']['Status']['StateChangeReason'])
        else:
            util.logger.info("Delete table/View:" + query_execution_status)
            time.sleep(sleepTime)
            sleepTime += 1
    
    return

def deleteS3Objects(location: str):
    
    if location == '' :
        util.logger.info("Nothing to delete in S3")
        return
    else:
        s3_client = boto3.client('s3', config=Config(connect_timeout=5, read_timeout=10, retries={'max_attempts': 10}))

        parsedLocation = urlparse(location, allow_fragments=False)
        bucket = parsedLocation.netloc
        prefix = parsedLocation.path
        prefix = prefix.replace('*.parquet','').replace('*/','')
        prefix = prefix.lstrip('/')

        response = s3_client.list_objects_v2(
            Bucket=bucket,
            MaxKeys=1000,   #This is AWS max
            Prefix=prefix,
            FetchOwner=False
        )    

        keysRetrieved = response['KeyCount']

        if keysRetrieved == 0:
            util.logger.info("No objects found to delete at " + location)
            return

        while keysRetrieved > 0:
            util.logger.info('Deleting ' + str(keysRetrieved) + ' objects ')

            keysList = []
            for thisFile in response['Contents']:
                keysList.append({'Key': thisFile['Key']})

            delete_response = s3_client.delete_objects(
                Bucket=bucket,
                Delete={'Objects': keysList}
            )

            if response.get('NextContinuationToken') == None:
                break
            else:
                response = s3_client.list_objects_v2(
                    Bucket=bucket,
                    Prefix=prefix,
                    ContinuationToken=response['NextContinuationToken'],
                    FetchOwner=False
                )

                keysRetrieved = response['KeyCount']
		
        util.logger.info('All objects deleted from ' + location)
        return

def updateHKOperationStatus(operationId: str): 
    operations_table: str = os.environ['ASCEND_HK_TABLE'].strip()

    try:
        dynamodbResource = boto3.resource('dynamodb', config=Config(connect_timeout=5, read_timeout=10, retries={'max_attempts': 10}))
        table = dynamodbResource.Table(operations_table)

        now = datetime.datetime.now()
        now_time_stamp = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        response = table.update_item(
            Key={
                'operation_id': operationId
            },
            UpdateExpression="set #st = :s, complete_ts = :c",
            ExpressionAttributeNames={
                "#st": "status"
            },
            ExpressionAttributeValues={
                ':s': 'COMPLETED',
                ':c': now_time_stamp
            },
            ReturnValues="UPDATED_NEW"
        )        

    except BaseException as error:
        util.logger.info('Error trying to update housekeeping operation: ' + operationId + ' in table: ' + operations_table)
        raise RuntimeError(error)


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
    
    
