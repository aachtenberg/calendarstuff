import os
import json
import logging
import time
from datetime import datetime, timedelta
import boto3

#def send_sns_notification(sns_client,topic_arn,subject,message):
#   response = sns_client.publish(TopicArn = topic_arn,Subject = subject,Message = message)

s3 = boto3.client('s3')
cloudwatch = boto3.client('cloudwatch')
cloudwatchlogs = boto3.client('logs')
logger = logging.getLogger()


redshift_user_count_sql = """select user_name, count(*) as user_count FROM stv_sessions where user_name != 'rdsdb' group by user_name"""
errlog_sql = """select * from stl_error where recordtime >= '{timestamp}' order by recordtime desc"""
datashare_audit_sql = """select * from sys_datashare_change_log where record_time >= '{timestamp}' order by record_time desc"""
datashare_queries = {
    "producer_sql" : """select * from sys_datashare_usage_producer where  record_time >= '{timestamp}' order by record_time desc""",
    "consumer_sql" : """select * from sys_datashare_usage_consumer where  record_time >= '{timestamp}' order by record_time desc"""
}

bucket_name = "rfdh-sbx01-cac1-s3"
bucket_prefix = 'frtb/error/' 

def list_s3_contents():
    try:
        logger.info(f"Listing objects in bucket: {bucket_name}")
        objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=bucket_prefix)

        if 'Contents' not in objects:
            logger.info(f"No objects found in s3 bucket: {bucket_name}")
            s3_contents  = ["No objects in bucket."]
        else:
            s3_contents = [obj['Key'] for obj in objects['Contents']]
        
        logger.info(f"S3 Bucket {bucket_name} contents: {s3_contents}")
    except Exception as e:
        logger.info(f"Error : {e}")
        raise

def extract_from_jdbc_url(jdbc_url):
    parts = jdbc_url.split('/')
    cluster_id = parts[2].split('.')[0]
    database=parts[-1]
    return cluster_id, database

def check_query_status(statement_id, client):
    while True:
      status = client.describe_statement(Id=statement_id)
      if status['Status'] in ['FINISHED', 'FAILED', 'ABORTED']:
        return status
      time.sleep(2)  # Wait for 2 seconds before checking again

def execute_sql_statement(redshift_client, redshift_database, redshift_user, redshift_cluster_id, sqlstatement):
    try:
        response = redshift_client.execute_statement(
        ClusterIdentifier=redshift_cluster_id,
        Database=redshift_database,
        DbUser=redshift_user,
        Sql=sqlstatement
        )
        return response
    except Exception as e:
        logger.info(f"Error executing sql statement: {sqlstatement}, {e}")
        raise

def redshift_datashare_activity_to_cloudwatch(redshift_client, redshift_database, redshift_user, redshift_cluster_id, interval, environment):
    if not interval:
        interval = 10
   
    hourago = datetime.now() - timedelta(minutes=int(interval))
    hourago_str = hourago.strftime('%Y-%m-%d %H:%M:%S')
    LOG_GROUP = f"/aws/lambda/RFDHRedshift-{environment}-{redshift_cluster_id}-datashare-logstream"
    LOG_STREAM = f"{datetime.now().strftime('%Y-%m-%d')}"

    try:
        cloudwatchlogs.create_log_group(logGroupName=LOG_GROUP)
    except cloudwatchlogs.exceptions.ResourceAlreadyExistsException:
        pass
    
    try:
        cloudwatchlogs.create_log_stream(logGroupName=LOG_GROUP, logStreamName=LOG_STREAM)
    except cloudwatchlogs.exceptions.ResourceAlreadyExistsException:
        pass
    
    try:
        for name, query in datashare_queries.items():
            querytoexec = query.format(timestamp=hourago_str)
            response = execute_sql_statement(redshift_client, redshift_database, redshift_user, redshift_cluster_id, querytoexec)
            statement_id = response['Id']
            status = check_query_status(statement_id, redshift_client)
            if status['Status'] != 'FINISHED':
                logger.info(f"ErrorLog Query for Redshift failed or aborted : {status}")
                return

            results = redshift_client.get_statement_result(Id=statement_id)
            
            seq_token = None
            for data in results["Records"]:
                data = str(data)
                # print(data)
                #Send the data to log group and log stream in cloud watch
                log_event = {
                    'logGroupName': LOG_GROUP,
                    'logStreamName': LOG_STREAM,
                    'logEvents': [
                        {
                        'timestamp': int(round(time.time() * 1000)),
                        'message': f"{name}:data"
                        }
                    ],
                }
                if seq_token:
                    log_event['sequenceToken'] = seq_token
                response = cloudwatchlogs.put_log_events(**log_event)
                seq_token = response['nextSequenceToken']
                time.sleep(1)
            return
    except Exception as e:
        logger.info(f"Error in retrieving datashare data: {e}")
        raise

def redshift_log_to_cloudwatch(redshift_client, redshift_database, redshift_user, redshift_cluster_id, interval, environment):
    if not interval:
        interval = 10
   
    hourago = datetime.now() - timedelta(minutes=int(interval))
    hourago_str = hourago.strftime('%Y-%m-%d %H:%M:%S')
    errsql = errlog_sql.format(timestamp=hourago_str)

    clusterid = 'rfdhcluster'
    LOG_GROUP = f"/aws/lambda/RFDHRedshift-{environment}-{redshift_cluster_id}-error-logstream"
    LOG_STREAM = f"{datetime.now().strftime('%Y-%m-%d')}"

    try:
        cloudwatchlogs.create_log_group(logGroupName=LOG_GROUP)
    except cloudwatchlogs.exceptions.ResourceAlreadyExistsException:
        pass
    
    try:
        cloudwatchlogs.create_log_stream(logGroupName=LOG_GROUP, logStreamName=LOG_STREAM)
    except cloudwatchlogs.exceptions.ResourceAlreadyExistsException:
        pass
    
    response = execute_sql_statement(redshift_client, redshift_database, redshift_user, redshift_cluster_id, errsql)
    try:
        statement_id = response['Id']
        status = check_query_status(statement_id, redshift_client)
        if status['Status'] != 'FINISHED':
            logger.info(f"ErrorLog Query for Redshift failed or aborted : {status}")
            return

        results = redshift_client.get_statement_result(Id=statement_id)
        
        seq_token = None
        for data in results["Records"]:
            data = str(data)
            # print(data)
            #Send the data to log group and log stream in cloud watch
            log_event = {
                'logGroupName': LOG_GROUP,
                'logStreamName': LOG_STREAM,
                'logEvents': [
                    {
                    'timestamp': int(round(time.time() * 1000)),
                    'message': data
                    }
                ],
            }
            if seq_token:
                log_event['sequenceToken'] = seq_token
            response = cloudwatchlogs.put_log_events(**log_event)
            seq_token = response['nextSequenceToken']
            time.sleep(1)
        return
    except Exception as e:
        logger.info(f"Error in retrieving user count: {e}")
        raise

def redshift_user_connections(redshift_client, redshift_database, redshift_user, redshift_cluster_id, environment):
    try:
        response = execute_sql_statement(redshift_client, redshift_database, redshift_user, redshift_cluster_id, redshift_user_count_sql)

        statement_id = response['Id']
        status = check_query_status(statement_id, redshift_client)
        if status['Status'] != 'FINISHED':
            return status

        # Wait for the query to complete (you might need to implement a loop or use wait() for longer queries)
        results = redshift_client.get_statement_result(Id=statement_id)
        
        logger.info(f"Results: {results}")
        # Process the results
        connections = []
        for row in results['Records']:
            user = row[0]['stringValue']
            count = int(row[1]['longValue'])
            connections.append({'user': user, 'connections': count})

            # Publish to CloudWatch as a custom metric
            cloudwatch.put_metric_data(
                Namespace='RedshiftConnections',
                MetricData=[
                    {
                        'MetricName': f"RFDH-{environment}-{redshift_cluster_id}-redshift-connectionsbyuser",
                        'Dimensions': [
                            {
                                'Name': 'User',
                                'Value': user
                            },
                        ],
                        'Value': count,
                        'Unit': 'Count'
                    },
                ]
            )
    except Exception as e:
        logger.info(f"Error in retrieving user count: {e}")
        raise


def lambda_handler(event, context):
    logger.setLevel(logging.INFO)
    logger.info(event)
    secret_name = os.getenv('secret_name')
    jdbc_url = os.getenv('jdbc_url')
    environment = os.getenv('environment')
    region_name = 'ca-central-1'
    action = event['db_event']
    
    try:
        cluster_id,database = extract_from_jdbc_url(jdbc_url)
        session = boto3.session.Session()
        secrets_client = session.client(service_name='secretsmanager',region_name = region_name)
        redshift_data = boto3.client('redshift-data',region_name = region_name)
        
        get_secret_value_response = secrets_client.get_secret_value(SecretId = secret_name)
        secret = get_secret_value_response['SecretString']
        secret_dict = json.loads(secret)
        redshift_user = secret_dict['masterUsername']
        redshift_password = secret_dict['masterUserPassword']

        match action:
            case "errorlog":
               interval = event['log_interval']
               redshift_log_to_cloudwatch(redshift_data, database, redshift_user, cluster_id, interval, environment)
            case "usercount":
               redshift_user_connections(redshift_data, database, redshift_user, cluster_id, environment)
            case "s3":
               list_s3_contents()
            case "datashare_log":
               interval = event['log_interval']
               redshift_datashare_activity_to_cloudwatch(redshift_data, database, redshift_user, cluster_id, interval, environment) 
            case _:
               logger.info(f"No arguments provided, defaulting to usercount")
               redshift_user_connections(redshift_data, database, redshift_user, cluster_id, environment)
    
    except Exception as e:
        logger.info(f"Error in Lambda Handler: {e}")
        raise

    return {
        'statusCode': 200,
        'body': json.dumps('Metrics successfully pushed to CloudWatch')
    }