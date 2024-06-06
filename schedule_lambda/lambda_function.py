import boto3
import json
from datetime import datetime, timedelta, timezone

client = boto3.client('scheduler')
schedule_mapping = {
    "-1-day": timedelta(days=1),
    "-5-hour": timedelta(hours=5),
    "-10-mins": timedelta(minutes=10),
}

def check_before_time(result_time):
    seoul = timezone(timedelta(hours=9))
    now = datetime.now(tz=seoul)
    return now <= result_time

def make_schedule_time(page_time, mapping_time):
    page_date_time = datetime.fromisoformat(page_time)
    return page_date_time - mapping_time

def create_eventbridge_rule(target_arn, page, slack_url):
    try:
        page_name = page['name']
        page_time = page['time']
        data = {
            "page": page,
            "slack_url": slack_url
        }
        for mapping_name, mapping_time in schedule_mapping.items():
            result_time = make_schedule_time(page_time, mapping_time)
            if not check_before_time(result_time):
                continue
            pasred_time = result_time.isoformat().split('+')[0]
            schedule_name = page_name + mapping_name
            response = client.create_schedule(
            Name=schedule_name,
            ScheduleExpression=f'at({pasred_time})',
            ScheduleExpressionTimezone='Asia/Seoul',
            ActionAfterCompletion='DELETE',
            FlexibleTimeWindow={
                'Mode': 'OFF'
            },
            Target={
                    'Arn': target_arn,
                    'RoleArn': 'arn:aws:iam::654654343720:role/eventbridge-sns',
                    'Input': json.dumps(data) if page else '{}'
            })
            print(response)
    except Exception as e:
        print(e)

def lambda_handler(event, context):
    records = event['Records']
    for record in records:
        body = json.loads(record['body'])
        page = body['page']
        slack_url = body['slack_url']
        create_eventbridge_rule('arn:aws:sns:ap-northeast-2:654654343720:toSender', page, slack_url)
    response = {
        'statusCode': 200
    }
    return response
