import json
import boto3
import requests
from opensearchpy import OpenSearch, RequestsHttpConnection

def detect_labels(photo, bucket):
    labels_res = []

    client=boto3.client('rekognition')

    response = client.detect_labels(Image={'S3Object':{'Bucket':bucket,'Name':photo}},
        MaxLabels=10)

    print('Detected labels for ' + photo) 
    
    for label in response['Labels']:
        print ("Label: " + label['Name'])
        labels_res.append(label['Name'])
        
    return labels_res

def index_into_es(index, type_doc, new_doc):
    awsauth = ('photos', 'Photos@123')
    endpoint = 'https://search-photos-hpsg6wkgwfqtxtblfoppuqgwn4.us-east-1.es.amazonaws.com/{}/{}'.format(index, type_doc)
    headers = {'Content-Type':'application/json'}
    res = requests.post(endpoint, auth = awsauth, data=new_doc, headers=headers)
    print(res.content)
    
def lambda_handler(event, context):

    print(event)
    
    bucket = "smart-album-b2"
    elasticURL = "https://search-photos-hpsg6wkgwfqtxtblfoppuqgwn4.us-east-1.es.amazonaws.com/"
    
    for record in event['Records']:
        image_name = record["s3"]["object"]["key"]
        print('image name ',image_name)
       
        labels_res=detect_labels(image_name, bucket)
        print('resulting labels ',labels_res)
        
        query={'objectKey':image_name,'bucket':'smart-album-b2','labels':labels_res}
        index_into_es('photos','photo',json.dumps(query))
        
    return {
        'labels_test': detect_labels(photo, "smart-album-b2"),
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
