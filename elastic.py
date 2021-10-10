from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3
import pickle
import random
import requests
import json
headers = {'Authorization':'Bearer '}
url = 'https://api.yelp.com/v3/businesses/search'
openSearchEndpoint = 'https://search-yelp-rest-2utd3mznqqcro7hgrfeubpknny.us-east-1.es.amazonaws.com/' 
region = 'us-east-1'
accessID=''
secretKey = ''
categories = ['indian', 'chinese', 'mexican', 'italian', 'french', 'japanese', 'american', 'continental', 'mediterrian']
citites = ['manhattan', 'seattle', 'brooklyn', 'san francisco', 'los angeles', 'austin', 'raleigh', 'bronx', 'queens', 'staten island']
service = 'es'
credentials = boto3.Session(region_name=region, aws_access_key_id=accessID, aws_secret_access_key=secretKey).get_credentials()
awsauth = AWS4Auth(accessID, secretKey, region, service, session_token=credentials.token)

search = OpenSearch(
    hosts = openSearchEndpoint,
    http_auth = awsauth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection
)

index_body = {
              'settings': {
                'index': {
                  'number_of_shards': 4
                }
              }
            }
response = search.indices.create('restaurant', body=index_body)   resp[i]['id']
print('\nCreating index:')

def writeInBatch(resp, cusine):
     for i in range(len(resp)):
         if(len(resp)==0):
             break;
         try:
             response = search.index(
            index = 'restaurant',
            body = {
                'restaurentID': resp['businesses'][0]['id'],
                'cuisine': cusine
            },
            refresh = True
            )

         except:
            print("chalo")
         

         
for i in categories:
    for j in citites:
        url_params = url_params = {
        'categories': i,
        'location': j,
        'limit': 2
        }
        response = requests.get(url, headers = headers, params = url_params)
        jsonResponse = json.loads(response.content.decode("utf-8"))
        writeInBatch(jsonResponse, i)
        print(response.content)
id = 1
