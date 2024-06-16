def lambda_handler(event, context):
    # 사용자 속성 가져오기
    user_attributes = event['request']['userAttributes']
    email = user_attributes.get('email')

    if not email or '@' not in email:
        raise Exception("Invalid email address")

    return event
