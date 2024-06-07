import json
from message_service import MessageService
from schema import NotionPage

def lambda_handler(event, context):
    message_service = MessageService()

    for record in event['Records']:
        message = json.loads(record['Sns']['Message'])
        slack_url = message['slack_url']
        schedule_name = message['schedule_name']
        page = NotionPage(**message['page'])
        message_service.make_and_send_message(page, slack_url, schedule_name)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
