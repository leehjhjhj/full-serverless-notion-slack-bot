from dynamodb import DynamoDBClient
import requests
from datetime import datetime, timezone, timedelta
from schema import NotionPage, StatusChoice
from boto3.dynamodb.conditions import Key
import boto3
import json

class ScanService:
    def __init__(self):
        self._connect_list_table = DynamoDBClient('notion_slack_list')
        self._schedule_table = DynamoDBClient('notion_slack_schedule')
        self._sqs = boto3.client('sqs')
    
    def scan_and_schedule_page(self):
        try:
            connect_list = self._connect_list_table.table.scan().get('Items')
            for connect in connect_list:
                if connect['status'] == "deactivate":
                    continue
                scheduled_page_ids = self._get_scheduled_page_ids(connect['connection_name'])
                results = self._scan_notion_database(connect['notion_api_key'], connect['notion_database_id'])
                for result in results:
                    page: NotionPage = self._farthing_calender_data(result, connect['connection_name'])
                    if self._check_can_schedule_page(page, scheduled_page_ids):
                        page.time = page.time.isoformat()
                        self._schedule_table.put_data(dict(page))
                        try:
                            response = self._send_message_to_sqs(page, connect['slack_url'])
                            print(f"send to sqs: {response}")
                        except Exception as e:
                            print(f"fail sending to sqs: {e}")
        except Exception as e:
            print(e)
    
    def _send_message_to_sqs(self, page: NotionPage, slack_url: str):
        response = self._sqs.send_message(
            QueueUrl='https://sqs.ap-northeast-2.amazonaws.com/654654343720/toScheduler',
            MessageBody=json.dumps({
                "page": dict(page),
                "slack_url": slack_url
            }),
        )
        return response

    def _scan_notion_database(self, notion_api_key: str, notion_database_id: str):
        token = notion_api_key
        database_id = notion_database_id
        headers = {
            "Authorization": "Bearer " + token,
            "Content-Type": "application/json",
            "Notion-Version": "2022-02-22"
        }
        readUrl = f"https://api.notion.com/v1/databases/{database_id}/query"
        res = requests.request("POST", readUrl, headers=headers)
        data = res.json()
        results = data.get('results')
        return results

    def _get_scheduled_page_ids(self, connection_name):
        query_params = {
            'KeyConditionExpression': Key('connection_name').eq(connection_name)
        }
        items = self._schedule_table.table.query(**query_params).get('Items', [])
        page_ids = [item['page_id'] for item in items]
        return set(page_ids)
        
    def _check_can_schedule_page(self, page, scheduled_page_ids):
        if not self._check_meeting_status(page.status):
            # print(page.name, "False1")
            return False
        if page.page_id in scheduled_page_ids:
            # print(page.name, "False2")
            return False
        if not self._check_meeting_time(page.time):
            # print(page.name, "False3")
            return False
        return True

    def _farthing_calender_data(self, scan_result, connection_name):
        properties = scan_result.get('properties', {})
        multi_select = properties.get("확정여부", {}).get("multi_select")
        if multi_select:
            status_str = multi_select[0].get("name")
        status_enum = StatusChoice(status_str) if status_str else None
        notion_database_id=scan_result.get("parent",{}).get("database_id").replace('-','')
        time = datetime.fromisoformat(properties.get("날짜", {}).get("date", {}).get("start"))
        page = NotionPage(
            connection_name=connection_name,
            page_id=scan_result.get('id'),
            notion_database_id=notion_database_id,
            status=status_enum,
            time=time,
            meeting_type=properties.get("종류", {}).get("multi_select", [{}])[0].get("name"),
            meeting_url=scan_result.get('url'),
            name=properties.get("이름", {}).get("title", [{}])[0].get('text').get('content'),
        )
        return page
    
    def time_to_utc9(self, time):
        return time
    
    def _check_meeting_status(self, status):
        if status == "확정":
            return True
        return False

    def _check_meeting_time(self, meeting_time):
        try:
            seoul = timezone(timedelta(hours=9))
            now = datetime.now(tz=seoul)
            day_after_tomorrow_start = (now + timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
            if now <= meeting_time < day_after_tomorrow_start:
                return True
            return False
        except TypeError as e:
            if str(e) == "can't compare offset-naive and offset-aware datetimes":
                print(f'시간 에러, 노션 DB 시간 수정 요망: {e}')
                return False
            else:
                print(f"시간 에러: {e}")
                return False