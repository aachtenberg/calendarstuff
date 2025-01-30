#!/bin/bash

# Bucket name where files will be uploaded
BUCKET_NAME="myaatest01"

# Function to create a test file
create_test_file() {
    local file_name=$1
    local content=$2
    echo "$content" > "$file_name"
}

# Function to upload file to S3
upload_to_s3() {
    local local_file=$1
    local s3_key=$2
    aws s3 cp "$local_file" "s3://$BUCKET_NAME/$s3_key"
}

# Create and upload monthly files
# Here we are using a pattern from your file_slo_mapping, adjust as necessary
MONTHLY_FILE_PREFIX="BCL_OPR_RISK_SMA_BCAR"
MONTHLY_FILE_SUFFIX=".dat.pgp"
CURRENT_YEAR=$(date +%Y)
CURRENT_MONTH=$(date +%m)

# Creating a test monthly file
MONTHLY_TEST_FILE="${MONTHLY_FILE_PREFIX}_${CURRENT_YEAR}${CURRENT_MONTH}${MONTHLY_FILE_SUFFIX}"
create_test_file "$MONTHLY_TEST_FILE" "This is a test monthly file content"
upload_to_s3 "$MONTHLY_TEST_FILE" "$MONTHLY_TEST_FILE"
echo "Uploaded monthly file: $MONTHLY_TEST_FILE"

# Create and upload daily files
# Here we're creating files for the current day, adjust the pattern if needed
DAILY_FILE_PREFIX="L1_PROFIT_CENTER_HIERARCHY_EXPLOSION"
DAILY_FILE_SUFFIX=".xlsx"
CURRENT_DAY=$(date +%d)

# Creating a test daily file
DAILY_TEST_FILE="${DAILY_FILE_PREFIX}_${CURRENT_YEAR}${CURRENT_MONTH}${CURRENT_DAY}${DAILY_FILE_SUFFIX}"
create_test_file "$DAILY_TEST_FILE" "This is a test daily file content"
upload_to_s3 "$DAILY_TEST_FILE" "$DAILY_TEST_FILE"
echo "Uploaded daily file: $DAILY_TEST_FILE"

# Clean up local test files
rm "$MONTHLY_TEST_FILE" "$DAILY_TEST_FILE"

echo "Test files uploaded to S3 bucket $BUCKET_NAME and local files removed."