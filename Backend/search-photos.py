import json
import boto3
import time
from botocore.vendored import requests
import requests

def lambda_handler(event, context):
    # TODO implement
    inputText = event["params"]["querystring"]["q"]
    keywords = get_keywords(inputText)
    image_array = get_image_locations(keywords)

    return {
        # 'check': event,
        'statusCode': 200,
        'headers':{
            'Access-Control-Allow-Origin':'*',
            'Access-Control-Allow-Credentials':True
        },
        'body': {"results":image_array}
    }


def get_keywords(inputstr):
    lex = boto3.client('lex-runtime')
    response = lex.post_text(
        botName = 'search',
        botAlias = 'searchObject',
        userId = 'searchPhotosLambda',
        inputText = inputstr
    )
    print(response['slots'])
    keywords = []
    slots = response['slots']
    keywords = [v for _, v in slots.items() if v]
    return keywords
    
def get_image_locations(keywords):
    endpoint = 'https://search-photos-hpsg6wkgwfqtxtblfoppuqgwn4.us-east-1.es.amazonaws.com/_search'
    headers = {'Content-Type': 'application/json'}
    awsauth = ('photos', 'Photos@123')
    prepared_q = []
    for k in keywords:
        prepared_q.append({"match": {"labels": k}})
    q = {"query": {"bool": {"should": prepared_q}}}
    r = requests.post(endpoint, auth = awsauth, headers=headers, data=json.dumps(q))
    print(r.json())
    
    image_array = []
    for each in r.json()['hits']['hits']:
        objectKey = each['_source']['objectKey']
        bucket = each['_source']['bucket']
        image_url = "https://" + bucket + ".s3.amazonaws.com/" + objectKey
        image_array.append(image_url)
        print(each['_source']['labels'])
    print(image_array)
    return image_array