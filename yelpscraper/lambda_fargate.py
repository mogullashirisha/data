import json
import boto3
import urllib.parse
import pymongo

def lambda_handler(event, context):
    # TODO implement

    #reading parameters
    name = event["queryStringParameters"]["name"]
    keyword = event["queryStringParameters"]["keyword"]
    city = event["queryStringParameters"]["city"]
    limit = event["queryStringParameters"]["limit"]
    
    #mongo
    status = 'Scraping Started'
    document = {'name': name,
                'limit': limit,
                'status': status,
                'created by':'UI',
                'created timestamp': ' ',
                'collection of email scraped':''
            }
    db = 'codemarket_akash'
    client = pymongo.MongoClient('mongodb+srv://sumi:'+urllib.parse.quote('sumi@')+'123@codemarket-staging.k16z7.mongodb.net/'+db+'?retryWrites=true&w=majority')
    database = client[db]
    collection = database['yelpscrapermailinglist']
    
    collection.insert_one(document)
    
    
    #encoding parameters
    keyword = urllib.parse.quote_plus(keyword)
    city = urllib.parse.quote_plus(city)
    
    
    #vairable definition
    cluster = 'yelpscraper'
    task_definition = 'yelpscraper:11'
    overrides = {"containerOverrides": [{'name':'docker_ec2','command':[name,keyword,city,limit]} ] }
    
    #running fargate task
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
    
    
    #response
    return {
        'statusCode': 200,
        'body': json.dumps(status)
    }
  

