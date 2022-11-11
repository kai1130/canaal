# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Purpose

AWS Lambda function that handles calls from an Amazon API Gateway REST API.
"""

import json
import boto3
import uuid

def lambda_handler(event, context):
    
    client = boto3.client('kinesis')
    
    body = json.loads(event['body'])
    chain = body['chainId']
    transfers = body['erc20Transfers']

    if transfers:
        for transfer in transfers:
            payload = {}
            payload['chain'] = chain
            payload['from'] = transfer['from']
            payload['to'] = transfer['to']
            payload['amount'] = transfer['valueWithDecimals']
            payload['symbol'] = transfer['tokenSymbol']

            print(payload)

            response = client.put_record(
                StreamName='moralis-input',
                Data=json.dumps(payload),
                PartitionKey=str(uuid.uuid4()))

    return {
        'statusCode': 200,
    }
