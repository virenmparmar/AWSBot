import json
import boto3

def lambda_handler(event, context):
    UserMessage = event.get('messages')
    print(UserMessage[0].get("unstructured").get("text"))
    client = boto3.client('lex-runtime')
    
    response = client.post_text(botName='Dining_Concierge_Chat_Bot',
                                botAlias='prod',
                                userId='viren',
                                inputText=UserMessage[0].get("unstructured").get("text"))
    print(response)
    print(response["message"])
    return {
        'statusCode': 200,
        'messages': [{
            'type' : 'unstructured',
            'unstructured' :{
                'text' : response["message"]
                }
            }]
    }
