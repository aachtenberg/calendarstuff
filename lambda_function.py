bucket_name = "your-s3-bucket-name"

now = get_est_time()
today = now.date()

# Check if today is a business day
holidays = us_public_holidays.get(today.year, [])
if not is_business_day(today, holidays):
    print(f"Today ({today}) is not a business day. Skipping daily file check.")
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
            # Check if the file exists in the S3 bucket
            try:
                # Get the file's metadata, including LastModified timestamp
                file_metadata = s3.head_object(Bucket=bucket_name, Key=expected_file_name)
                upload_time = file_metadata['LastModified'].replace(tzinfo=None)

                # Extract the timestamp from the filename
                filename_timestamp = re.search(r"(\d{8})\.xlsx", expected_file_name).group(1)
                filename_date = datetime.strptime(filename_timestamp, "%Y%m%d").date()

                # Validate the filename timestamp
                if filename_date != today:
                    alert_message = (
                        f"File {expected_file_name} has an incorrect timestamp. "
                        f"Expected: {today.strftime('%Y%m%d')}, Found: {filename_timestamp}"
                    )
                    print(alert_message)
                    send_alert(alert_message)
                else:
                    # Check if the file arrived on or before the expected arrival time
                    if upload_time <= expected_arrival_time:
                        print(f"File {expected_file_name} arrived on time: {upload_time}")
                    else:
                        alert_message = (
                            f"File {expected_file_name} arrived late. "
                            f"Expected by: {expected_arrival_time}, Arrived on: {upload_time}"
                        )
                        print(alert_message)