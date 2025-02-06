import boto3
from datetime import datetime, timedelta, time
import json
import logging

s3 = boto3.client('s3')
sns = boto3.client('sns')
cloudwatch = boto3.client('cloudwatch')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def load_json_from_s3(bucket_name, key):
    # Load JSON data from an S3 bucket.
    response = s3.get_object(Bucket=bucket_name, Key=key)
    data = json.loads(response['Body'].read().decode('utf-8'))
    return data

def parse_slo_mapping(slo_mapping_data):
    # Parse the SLO mapping data and convert slo_time to time objects.
    for pattern, slo in slo_mapping_data.items():
        slo['slo_time'] = datetime.strptime(slo['slo_time'], '%H:%M').time()
    return slo_mapping_data

def is_holiday(date, holidays):
    # Check if a date is a holiday.
    return date in holidays

def is_business_day(date, holidays):
    # Check if a date is a business day (Monday to Friday and not a holiday).
    return date.weekday() < 5 and not is_holiday(date, holidays)

def get_nth_business_day(year, month, n, holidays):
    # Calculate the nth business day of the month, excluding weekends and holidays.
    date = datetime(year, month, 1)  # Start from the first day of the month
    business_days = 0
    while business_days < n:
        if date.weekday() < 5 and not is_holiday(date, holidays):
            business_days += 1
        if business_days < n:
            date += timedelta(days=1)  # Move to the next day
    return date
  
def get_expected_arrival_time(date, slo_time):
    # Get the expected arrival time for a given date.
    return datetime.combine(date, slo_time)

def get_est_time():
    # Get the current time in EST (UTC-5).
    utc_now = datetime.utcnow()
    est_offset = timedelta(hours=-5)  # EST is UTC-5
    return utc_now + est_offset

def send_alert(message, sns_topic_arn):
    # Send an alert via SNS.
    #sns.publish(TopicArn=sns_topic_arn, Message=message, Subject="File Arrival Alert")
    return

def add_slo_status_tag(bucket_name, key, status):
    # Add an SLO status tag to the specified S3 object.
    s3.put_object_tagging(
        Bucket=bucket_name,
        Key=key,
        Tagging={
            'TagSet': [
                {
                    'Key': 'slo_status',
                    'Value': status
                }
            ]
        }
    )

def put_cloudwatch_metric(metric_name, value, reason=None):
    # Put a custom metric to CloudWatch.
    metric_data = {
        'MetricName': metric_name,
        'Value': value,
        'Unit': 'Count'
    }
    if reason:
        metric_data['Dimensions'] = [{'Name': 'Reason', 'Value': reason}]
    
    cloudwatch.put_metric_data(
        Namespace='FileSLO-Metrics',
        MetricData=[metric_data]
    )

def check_monthly_files(holidays, bucket_name, sns_topic_arn, file_slo_mapping):
    # Check for missing monthly files based on SLOs, using specified holidays.
    today = datetime.now()
    year = today.year
    month = today.month

    for pattern, slo in file_slo_mapping.items():
        if "dat.pgp" in pattern:  # Only process monthly files
            slo_days = slo["slo_days"]
            slo_time = slo["slo_time"]

            # Calculate the expected arrival deadline
            expected_arrival_date = get_nth_business_day(year, month, slo_days, holidays)
            expected_arrival_time = get_expected_arrival_time(expected_arrival_date, slo_time)

            # Check if the deadline has passed
            if today > expected_arrival_time:
                # Construct the expected file name
                expected_file_name = pattern.replace("*", f"{year}{month:02d}")

                s3_key = f"archive/{expected_file_name}"
                try:
                    # Check if the file exists in the S3 bucket
                    file_metadata = s3.head_object(Bucket=bucket_name, Key=s3_key)
                    last_modified = file_metadata['LastModified'].replace(tzinfo=None)

                    if last_modified <= expected_arrival_time:
                        logger.info(f"File {expected_file_name} exists and arrived on time. SLO met.")
                        add_slo_status_tag(bucket_name, s3_key, 'met')
                        put_cloudwatch_metric('MonthlySLOMet', 1)
                    else:
                        alert_message = (
                            f"File {expected_file_name} exists but arrived late. "
                            f"SLO not met. Expected by: {expected_arrival_time}, Arrived on: {last_modified}"
                        )
                        logger.info(alert_message)
                        send_alert(alert_message, sns_topic_arn)
                        add_slo_status_tag(bucket_name, s3_key, 'not met')
                        put_cloudwatch_metric('MonthlySLONotMet', 1, 'LateArrival')

                except s3.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == '404':
                        # File not found
                        alert_message = (
                            f"File {expected_file_name} is missing in the archive folder. "
                            f"SLO not met. Expected by: {expected_arrival_time}"
                        )
                        logger.info(alert_message)
                        send_alert(alert_message, sns_topic_arn)
                        put_cloudwatch_metric('MonthlySLONotMet', 1, 'FileNotFound')
                    else:
                        # Other S3 error
                        logger.info(f"Error checking file {expected_file_name}: {e}")

def check_daily_files(holidays, bucket_name, sns_topic_arn, file_slo_mapping):
    # Check for missing daily files based on SLOs, using specified holidays.
    # Get the current date and time in EST
    now = get_est_time()
    today = now.date()

    # Check if today is a business day
    if not is_business_day(today, holidays):
        logger.info(f"Today ({today}) is not a business day. Skipping daily file check.")
        return

    # Process daily files
    for pattern, slo in file_slo_mapping.items():
        if "xlsx" in pattern:  # Only process daily files
            slo_time = slo["slo_time"]

            # Construct the expected file name
            expected_file_name = pattern.replace("*", today.strftime('%Y%m%d'))

            # Calculate the expected arrival time (10:30 AM EST)
            expected_arrival_time = get_expected_arrival_time(today, slo_time)

            # Check if the deadline has passed
            if now > expected_arrival_time:
                s3_key = f"archive/{expected_file_name}"
                try:
                    # Check if the file exists in the S3 bucket
                    file_metadata = s3.head_object(Bucket=bucket_name, Key=s3_key)
                    last_modified = file_metadata['LastModified'].replace(tzinfo=None)

                    if last_modified <= expected_arrival_time:
                        logger.info(f"File {expected_file_name} exists and arrived on time. SLO met.")
                        add_slo_status_tag(bucket_name, s3_key, 'met')
                        put_cloudwatch_metric('DailySLOMet', 1)
                    else:
                        alert_message = (
                            f"File {expected_file_name} exists but arrived late. "
                            f"SLO not met. Expected by: {expected_arrival_time}, Arrived on: {last_modified}"
                        )
                        logger.info(alert_message)
                        send_alert(alert_message, sns_topic_arn)
                        add_slo_status_tag(bucket_name, s3_key, 'not met')
                        put_cloudwatch_metric('DailySLONotMet', 1, 'LateArrival')

                except s3.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == '404':
                        # File not found
                        alert_message = (
                            f"File {expected_file_name} is missing in the archive folder. "
                            f"SLO not met. Expected by: {expected_arrival_time}"
                        )
                        logger.info(alert_message)
                        send_alert(alert_message, sns_topic_arn)
                        put_cloudwatch_metric('DailySLONotMet', 1, 'FileNotFound')
                    else:
                        # Other S3 error
                        logger.info(f"Error checking file {expected_file_name}: {e}")

def lambda_handler(event, context):
    # Load holidays from S3
    bucket_name = event['bucket_name']
    sns_topic_arn = event['sns_topic_arn']
    holidays_file_key = event['holidays_file_key']
    slo_mapping_file_key = event['slo_mapping_file_key']
    
    holidays_data = load_json_from_s3(bucket_name, holidays_file_key)
    slo_mapping_data = load_json_from_s3(bucket_name, slo_mapping_file_key)
    file_slo_mapping = parse_slo_mapping(slo_mapping_data)
    
    # Switch based on if we want to check for Canadian or US holidays
    use_canadian_holidays = event.get('useCanadianHolidays', False)
    year = datetime.now().year
    
    if use_canadian_holidays:
        holidays = [datetime.strptime(date, '%Y-%m-%d') for date in holidays_data['ca_public_holidays'].get(str(year), [])]
    else:
        holidays = [datetime.strptime(date, '%Y-%m-%d') for date in holidays_data['us_public_holidays'].get(str(year), [])]
    
    # Use these holidays for checking business days
    check_monthly_files(holidays, bucket_name, sns_topic_arn, file_slo_mapping)
    check_daily_files(holidays, bucket_name, sns_topic_arn, file_slo_mapping)

    return {
        'statusCode': 200,
        'body': 'File check completed.'
    }