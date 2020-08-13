import json
import boto3
import re
from validate_email import validate_email
import pymongo
import urllib.parse


def lambda_handler(event, context):
    #read inputs
    userid = event["queryStringParameters"]["user_id"]
    name = event["queryStringParameters"]["name"]

    #mongo
    client = pymongo.MongoClient('mongodb+srv://sumi:'+urllib.parse.quote('sumi@')+'123@codemarket-staging.k16z7.mongodb.net/codemarket_akash?retryWrites=true&w=majority')
    db = client.get_database('codemarket_akash')
    collection = db['yelpscrapermailinglist']
    status = 'Cleaning Started'
    
    query = {"user_id":userid,"name":name}
    newvalues = {'$set':{'status':'Cleaning Started'}}
    collection.update_one(query,newvalues)
    
    #variable definition
    cluster = 'yelpscraper'
    task_definition = 'clean_email:1'
    overrides = {"containerOverrides": [{'name':'clean_email','command':[userid,name]} ] }
    
    #run task
    result = boto3.client('ecs').run_task(
    cluster=cluster,
    taskDefinition=task_definition,
    overrides=overrides,
    launchType = 'FARGATE',
    platformVersion='LATEST',
    networkConfiguration={
        'awsvpcConfiguration': {
            'subnets': [
                'subnet-014b0e273a8ba6353'
            ],
            'assignPublicIp': 'ENABLED'
        }
    },
    count=1,
    startedBy='lambda'
    )
      
    return {
        'statusCode': 200,
        'body': json.dumps(status)
    }

