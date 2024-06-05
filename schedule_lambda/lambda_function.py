import boto3
import json
from datetime import datetime, timedelta, timezone

client = boto3.client('scheduler')
lambda_client = boto3.client('lambda')

def make_schedule_time(minute: int):
    seoul = timezone(timedelta(hours=9))
    return (datetime.now(tz=seoul)+ timedelta(minutes=minute)).strftime('%Y-%m-%dT%H:%M:%S')

def create_eventbridge_rule(rule_name, target_arn, input_payload):
    # EventBridge 규칙 생성
    response = client.create_schedule(
    Name=rule_name,
    ScheduleExpression=f'at({make_schedule_time(5)})',
    ScheduleExpressionTimezone='Asia/Seoul',
    ActionAfterCompletion='DELETE',
    FlexibleTimeWindow={
        'Mode': 'OFF'
    },
    Target={
            'Arn': target_arn,
            'RoleArn': 'arn:aws:iam::831576238138:role/eventbridge-lambda',
            'Input': json.dumps(input_payload) if input_payload else '{}'
    })
    print(response)

def lambda_handler(event, context):
    
    create_eventbridge_rule('test-scheduler', 'arn:aws:lambda:ap-northeast-2:831576238138:function:eventbridge-test', None)
    response = {
        'statusCode': 200
    }
    return response
