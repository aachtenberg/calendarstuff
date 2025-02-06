import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch';

export class LambdaCdkStack extends cdk.Stack {
    constructor(scope: Construct, id: string, props?: cdk.StackProps) {
        super(scope, id, props);

        const bucketName = 'myaatest01';

        let bucket: s3.IBucket;
        try {
            // Try to import the existing bucket
            bucket = s3.Bucket.fromBucketName(this, 'ExistingBucket', bucketName);
        } catch (error) {
            // If not found, create a new one
            bucket = new s3.Bucket(this, 'NewBucket', {
                bucketName: bucketName,
                removalPolicy: cdk.RemovalPolicy.RETAIN, // Prevents accidental deletion
            });
        }

        // Define the Lambda function
        const lambdaFunction = new lambda.Function(this, 'S3FileSLOChecker', {
            runtime: lambda.Runtime.PYTHON_3_9,  // Use a compatible runtime
            handler: 'lambda_function.lambda_handler',
            code: lambda.Code.fromAsset('lambda'), // 'lambda' folder contains code
            environment: {
                BUCKET_NAME: bucket.bucketName
            }
        });

        // Grant Lambda permissions to read from S3
        bucket.grantRead(lambdaFunction);

        // Grant Lambda permissions to put object tagging in S3
        bucket.grantPut(lambdaFunction);

        // Grant Lambda permissions to publish to SNS (if needed)
        lambdaFunction.addToRolePolicy(new iam.PolicyStatement({
            actions: ['sns:Publish'],
            resources: ['arn:aws:sns:ca-central-1:507525864454:aatesttopic']
        }));

        // Grant Lambda permissions to put metrics to CloudWatch
        lambdaFunction.addToRolePolicy(new iam.PolicyStatement({
            actions: ['cloudwatch:PutMetricData'],
            resources: ['*']
        }));

        // Create an EventBridge rule to trigger the Lambda function every 5 minutes
        const rule = new events.Rule(this, 'Rule', {
            schedule: events.Schedule.rate(cdk.Duration.minutes(5))
        });

        rule.addTarget(new targets.LambdaFunction(lambdaFunction, {
            event: events.RuleTargetInput.fromObject({
                bucket_name: bucket.bucketName,
                sns_topic_arn: 'arn:aws:sns:ca-central-1:507525864454:aatesttopic',
                holidays_file_key: 'files/holidays.json',
                slo_mapping_file_key: 'files/file_slo_mapping.json'
            })
        }));

        // Create a CloudWatch dashboard
        const dashboard = new cloudwatch.Dashboard(this, 'FileSLODashboard', {
            dashboardName: 'FileSLODashboard'
        });

        // Add a Logs Table widget to the dashboard
        const logGroupName = `/aws/lambda/${lambdaFunction.functionName}`;
        const queryString = `
fields @timestamp, @message
| filter @message like /SLO met/ or @message like /SLO not met/
| parse @message "* File * exists and arrived on time. SLO met." as fileName1
| parse @message "* File * exists but arrived late. SLO not met." as fileName2
| parse @message "* File * is missing in the archive folder. SLO not met." as fileName3
| display @timestamp, coalesce(fileName1,fileName2,fileName3), @message
| sort @timestamp desc
| limit 20
`;

        dashboard.addWidgets(
            new cloudwatch.TextWidget({
                markdown: '# SLO Status for Files',
                width: 24
            }),
            new cloudwatch.LogQueryWidget({
                title: 'SLO Status',
                logGroupNames: [logGroupName],
                view: cloudwatch.LogQueryVisualizationType.TABLE,
                queryString: queryString,
                width: 24,
                height: 12
            })
        );

        // Output the Lambda function ARN
        new cdk.CfnOutput(this, 'LambdaFunctionArn', {
            value: lambdaFunction.functionArn
        });
    }
}
