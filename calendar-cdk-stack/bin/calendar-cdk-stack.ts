#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { LambdaCdkStack } from '../lib/lambda-cdk-stack'; // Ensure this path is correct

const app = new cdk.App();
new LambdaCdkStack(app, 'LambdaCdkStack', {
    env: {
        account: process.env.CDK_DEFAULT_ACCOUNT || '507525864454',
        region: process.env.CDK_DEFAULT_REGION || 'ca-central-1'
    }
});
