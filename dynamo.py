import boto3
import requests
import json
from decimal import Decimal
import datetime

categories = ['indian', 'chinese', 'mexican', 'italian', 'french', 'japanese', 'american', 'continental', 'mediterrian']
citites = ['manhattan', 'seattle', 'brooklyn', 'san francisco', 'los angeles', 'austin', 'raleigh', 'bronx', 'queens', 'staten island']
# categories = ['indian']
# citites = ['manhattan']
url = 'https://api.yelp.com/v3/businesses/search'
headers = {'Authorization':'Bearer '}

dynamodb = boto3.resource('dynamodb',region_name='us-east-1', aws_access_key_id='', aws_secret_access_key='')
table = dynamodb.Table('yelp-restaurant')

def writeInBatch(responses):
    resp = responses['businesses']
    print(resp)
    with table.batch_writer() as batch:
        for i in range(len(resp)):
            batch.put_item(
                Item={
                    'business_id': resp[i]['id'],
                    'name': resp[i]['name'],
                    'address': resp[i]['location']['display_address'],
                    'latitude': Decimal(str(resp[i]['coordinates']['latitude'])),
                    'longitude': Decimal(str(resp[i]['coordinates']['longitude'])),
                    'num_of_reviews':resp[i]['review_count'],
                    'rating': Decimal(str(resp[i]['rating'])),
                    'zip_code' : resp[i]['location']['zip_code'],
                    'insertedtimestamp' : str(datetime.datetime.now())
                }
            )




for i in categories:
    for j in citites:
        url_params = url_params = {
        'categories': i,
        'location': j,
        'limit': 2
        }
        response = requests.get(url, headers = headers, params = url_params)
        jsonResponse = json.loads(response.content.decode("utf-8"))
        writeInBatch(jsonResponse)
        print(response.content)
