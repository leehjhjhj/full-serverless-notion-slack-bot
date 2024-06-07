import boto3

class DynamoDBClient:
    def __init__(self, table_name: str):
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table(table_name)
        
    def put_data(self, input_data):
        self.table.put_item(
            Item =  input_data
        )