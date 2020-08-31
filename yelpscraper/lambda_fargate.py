import json
import boto3
import urllib.parse
import pymongo

def lambda_handler(event, context):
    # TODO implement

    #reading parameters
    user_id = event["queryStringParameters"]["user"]
    name = event["queryStringParameters"]["name"]
    keyword = event["queryStringParameters"]["keyword"]
    loc = event["queryStringParameters"]["loc"]
    limit = event["queryStringParameters"]["limit"]
    
    #mongo
    db = 'codemarket_devasish'
    client = pymongo.MongoClient('mongodb+srv://sumi:'+urllib.parse.quote('sumi@')+'123@codemarket-staging.k16z7.mongodb.net/'+db+'?retryWrites=true&w=majority')
    database = client[db]
    collection = database['yelpscrapermailinglist']
    
    status = 'Scraping Started'
    query = {'user_id':user_id,'name':name}
    document = collection.find_one(query)
    
    if document:
        newvalues = {"$set":{"status":status}}
        collection.update_one(query,newvalues)
    else:
        document = {'user_id':user_id,
                    'name': name,
                    'keyword':keyword,
                    'loc':loc,
                    'limit': limit,
                    'status': status,
                    'created by':'UI',
                    'created timestamp': '',
                    'collection of email scraped':''
                }
        
        collection.insert_one(document)

    #encoding parameters
    keyword = urllib.parse.quote_plus(keyword)
    loc = urllib.parse.quote_plus(loc)
    
    #vairable definition
    cluster = 'yelpscraper'
    task_definition = 'yelpscraper:12'
    overrides = {"containerOverrides": [{'name':'docker_ec2','command':[keyword,loc,limit]} ] }
   
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