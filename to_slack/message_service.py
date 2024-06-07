import requests
from schema import NotionPage
from dynamodb import DynamoDBClient
from boto3.dynamodb.conditions import Key
import json

class MessageService:
    def __init__(self):
        self._schedule_table = DynamoDBClient('notion_slack_schedule')

    def make_and_send_message(self, page: NotionPage, slack_url: str, schedule_name: str):
        try:
            if self._check_status(page):
                message = self._make_slack_message(page, schedule_name)
                self._send_message(message, slack_url)
        except Exception as e:
            print(f"오류 발생: {e}")

    def _make_slack_message(self, page: NotionPage, schedule_name: str):
        transformed_schedule_name = '-'.join(schedule_name.split('-')[-2:])
        transformed_date = self._transform_date(page.time)
        if transformed_schedule_name == "1-day":
            message = f">:bell: {transformed_date}에 \"{page.name}\"이/가 예정되어있어요! 잊지 마세요. <!channel> \n" \
                f"> 회의 타입: `{page.meeting_type}`\n" \
                f"> 회의 노션페이지: <{page.meeting_url}|바로가기>\n"
            
        elif transformed_schedule_name == "5-hour":
            message = f">:bell: 오늘 \"{page.name}\"이/가 있다는 거 잊지 않으셨죠? 잊지 말아주세요! <!channel> \n" \
                f"> 회의 타입: `{page.meeting_type}`\n" \
                f"> 회의 노션페이지: <{page.meeting_url}|바로가기>\n"
        else:
            message = f">:bangbang: 곧 10분 뒤에 \"{page.name}\" 가 시작돼요! 모두 준비해주세요. <!channel> \n" \
                f"> 회의 노션페이지: <{page.meeting_url}|바로가기>\n"
        return message

    def _send_message(self, message: str, slack_url: str):
        headers = {"Content-type": "application/json"}
        payload = {
        "blocks": [{
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": message
            }}]
        }
        response = requests.post(slack_url, headers=headers, data=json.dumps(payload))
        print(response)

    def _check_status(self, page: NotionPage):
        query_params = {
            'KeyConditionExpression': Key('connection_name').eq(page.connection_name) & Key('page_id').eq(page.page_id),
        }
        response = self._schedule_table.table.query(**query_params).get('Items', [])
    
        if not response:
            return False
        
        page_status = response[0].get('status')
        if page_status == "취소 및 변경":
            return False
    
        return True

    def _transform_date(self, time):
        am_pm = "오후" if time.hour >= 12 else "오전"
        hour_12 = time.hour - 12 if time.hour > 12 else time.hour
        formatted_time = f"{time.month}월 {time.day}일 {am_pm} {hour_12}시 {time.minute}분"
        return formatted_time