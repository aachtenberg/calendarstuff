import boto3
from datetime import datetime, timedelta, time
import re
import logging

s3 = boto3.client('s3')
sns = boto3.client('sns')
sns_topic_arn = 'arn:aws:sns:ca-central-1:507525864454:aatesttopic' 
bucket_name = 'myaatest01'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Define file patterns and their SLOs
file_slo_mapping = {
    # Monthly files
    "BCL_OPR_RISK_SMA_BCAR.*.dat.pgp": {
        "slo_days": 8,  # 8th business day
        "slo_time": time(17, 0)  # 5:00 PM
    },
    "ANOTHER_FILE_PATTERN.*.dat.pgp": {
        "slo_days": 5,  # 5th business day
        "slo_time": time(12, 0)  # 12:00 PM
    },
    "YET_ANOTHER_PATTERN.*.dat.pgp": {
        "slo_days": 3,  # 3rd business day
        "slo_time": time(9, 0)  # 9:00 AM
    },
    # Daily files
    "L1_PROFIT_CENTER_HIERARCHY_EXPLOSION_*.xlsx": {
        "slo_days": 0,  # Same day
        "slo_time": time(10, 30)  # 10:30 AM EST
    },
    "L1_PROFIT_CENTER_*.xlsx": {
        "slo_days": 0,  # Same day
        "slo_time": time(10, 30)  # 10:30 AM EST
    },
    "L1_PROFIT_CENTER_CATEGORY_*.xlsx": {
        "slo_days": 0,  # Same day
        "slo_time": time(10, 30)  # 10:30 AM EST
    },
    "L1_NATURAL_GL_*.xlsx": {
        "slo_days": 0,  # Same day
        "slo_time": time(10, 30)  # 10:30 AM EST
    },
    "L1_MDM_US_GAAP_GL_CATEGORY_*.xlsx": {
        "slo_days": 0,  # Same day
        "slo_time": time(10, 30)  # 10:30 AM EST
    },
    "L1_US_GAAP_GL_CATEGORY_EXPLOSION_*.xlsx": {
        "slo_days": 0,  # Same day
        "slo_time": time(10, 30)  # 10:30 AM EST
    }
}

# Define US public holidays for the relevant year(s)
us_public_holidays = {
    2025: [
        datetime(2025, 1, 1),   # New Year's Day
        datetime(2025, 1, 20),  # Martin Luther King Jr. Day
        datetime(2025, 2, 17),  # Presidents' Day
        datetime(2025, 5, 26),  # Memorial Day
        datetime(2025, 6, 19),  # Juneteenth
        datetime(2025, 7, 4),   # Independence Day
        datetime(2025, 9, 1),   # Labor Day
        datetime(2025, 10, 13), # Columbus Day
        datetime(2025, 11, 11), # Veterans Day
        datetime(2025, 11, 27), # Thanksgiving Day
        datetime(2025, 12, 25), # Christmas Day
    ],
    # Add more years as needed
}

# Define Canadian holidays for 2025
ca_public_holidays = {
    2025: [
        datetime(2025, 1, 1),   # New Year's Day
        datetime(2025, 1, 2),   # New Year's Day Observed in Quebec (since Jan 1 is a Wednesday)
        datetime(2025, 2, 17),  # Family Day (example for some provinces, adjust as necessary)
        datetime(2025, 4, 18),  # Good Friday
        datetime(2025, 5, 19),  # Victoria Day
        datetime(2025, 7, 1),   # Canada Day
        datetime(2025, 8, 4),   # Civic Holiday
        datetime(2025, 9, 1),   # Labour Day
        datetime(2025, 9, 30),  # National Day for Truth and Reconciliation
        datetime(2025, 10, 13), # Thanksgiving
        datetime(2025, 12, 25), # Christmas Day
        datetime(2025, 12, 26), # Boxing Day
    ]
}

def is_holiday(date, holidays):
    """Check if a date is a holiday."""
    return date in holidays

def is_business_day(date, holidays):
    """Check if a date is a business day (Monday to Friday and not a holiday)."""
    return date.weekday() < 5 and not is_holiday(date, holidays)

def get_nth_business_day(year, month, n, holidays):
    """Calculate the nth business day of the month, excluding weekends and holidays."""
    date = datetime(year, month, 1)  # Start from the first day of the month
    business_days = 0
    while business_days < n:
        if date.weekday() < 5 and not is_holiday(date, holidays):
            business_days += 1
        if business_days < n:
            date += timedelta(days=1)  # Move to the next day
    return date

def get_expected_arrival_time(date, slo_time):
    """Get the expected arrival time for a given date."""
    return datetime.combine(date, slo_time)

def get_est_time():
    """Get the current time in EST (UTC-5)."""
    utc_now = datetime.utcnow()
    est_offset = timedelta(hours=-5)  # EST is UTC-5
    return utc_now + est_offset

def send_alert(message):
    """Send an alert via SNS."""
    #sns.publish(TopicArn=sns_topic_arn, Message=message, Subject="File Arrival Alert")
    return

def add_slo_status_tag(bucket_name, key, status):
    """Add an SLO status tag to the specified S3 object."""
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

def check_monthly_files(holidays):
    """Check for missing monthly files based on SLOs, using specified holidays."""
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
                    else:
                        alert_message = (
                            f"File {expected_file_name} exists but arrived late. "
                            f"SLO not met. Expected by: {expected_arrival_time}, Arrived on: {last_modified}"
                        )
                        logger.info(alert_message)
                        send_alert(alert_message)
                        add_slo_status_tag(bucket_name, s3_key, 'not met')

                except s3.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == '404':
                        # File not found
                        alert_message = (
                            f"File {expected_file_name} is missing in the archive folder. "
                            f"SLO not met. Expected by: {expected_arrival_time}"
                        )
                        logger.info(alert_message)
                        send_alert(alert_message)
                    else:
                        # Other S3 error
                        logger.info(f"Error checking file {expected_file_name}: {e}")

def check_daily_files(holidays):
    """Check for missing daily files based on SLOs, using specified holidays."""
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
                    else:
                        alert_message = (
                            f"File {expected_file_name} exists but arrived late. "
                            f"SLO not met. Expected by: {expected_arrival_time}, Arrived on: {last_modified}"
                        )
                        logger.info(alert_message)
                        send_alert(alert_message)
                        add_slo_status_tag(bucket_name, s3_key, 'not met')

                except s3.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == '404':
                        # File not found
                        alert_message = (
                            f"File {expected_file_name} is missing in the archive folder. "
                            f"SLO not met. Expected by: {expected_arrival_time}"
                        )
                        logger.info(alert_message)
                        send_alert(alert_message)
                    else:
                        # Other S3 error
                        logger.info(f"Error checking file {expected_file_name}: {e}")

def lambda_handler(event, context):
    # Switch based on if we want to check for Canadian or US holidays
    use_canadian_holidays = event.get('useCanadianHolidays', False)
    year = datetime.now().year
    
    if use_canadian_holidays:
        holidays = ca_public_holidays.get(year, [])
    else:
        holidays = us_public_holidays.get(year, [])
    
    # Use these holidays for checking business days
    check_monthly_files(holidays)
    check_daily_files(holidays)

    return {
        'statusCode': 200,
        'body': 'File check completed.'
    }