import boto3
from datetime import datetime, timedelta, time

s3 = boto3.client('s3')
sns = boto3.client('sns')
sns_topic_arn = 'arn:aws:sns:ca-central-1:507525864454:aatesttopic'  # Replace with your SNS topic ARN

# Define file patterns, their SLOs, and the specific time of day
file_slo_mapping = {
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

def is_holiday(date, holidays):
    """Check if a date is a holiday."""
    return date in holidays

def get_nth_business_day(year, month, n, holidays):
    """Calculate the nth business day of the month, excluding weekends and holidays."""
    date = datetime(year, month, 1)  # Start from the first day of the month
    business_days = 0
    while business_days < n:
        # Check if the date is a weekday (Monday to Friday) and not a holiday
        if date.weekday() < 5 and not is_holiday(date, holidays):
            business_days += 1
        if business_days < n:
            date += timedelta(days=1)  # Move to the next day
    return date

def send_alert(message):
    """Send an alert via SNS."""
    sns.publish(
        TopicArn=sns_topic_arn,
        Message=message,
        Subject="File Arrival Alert"
    )

def check_missing_files():
    """Check for missing files based on SLOs."""
    today = datetime.now()
    year = today.year
    month = today.month

    # Get the list of holidays for the year
    holidays = us_public_holidays.get(year, [])

    for pattern, slo in file_slo_mapping.items():
        slo_days = slo["slo_days"]
        slo_time = slo["slo_time"]

        # Calculate the expected arrival deadline
        expected_arrival_date = get_nth_business_day(year, month, slo_days, holidays)
        expected_arrival_time = datetime.combine(expected_arrival_date, slo_time)

        # Check if the deadline has passed
        if today > expected_arrival_time:
            # Construct the expected file name
            expected_file_name = pattern.replace("*", f"{year}{month:02d}")

            # Check if the file exists in the S3 bucket
            try:
                s3.head_object(Bucket='myaatest01', Key=expected_file_name)
                print(f"File {expected_file_name} arrived on time.")
            except s3.exceptions.ClientError as e:
                if e.response['Error']['Code'] == '404':
                    # File not found
                    alert_message = (
                        f"File {expected_file_name} is missing. "
                        f"Expected by: {expected_arrival_time}"
                    )
                    print(alert_message)
                    send_alert(alert_message)
                else:
                    # Other S3 error
                    print(f"Error checking file {expected_file_name}: {e}")

def lambda_handler(event, context):
    check_missing_files()
    return {
        'statusCode': 200,
        'body': 'Missing file check completed.'
    }