import json
import boto3
import urllib.parse
import pymongo

def lambda_handler(event, context):
    # TODO implement

    #reading parameters
    userid = event["queryStringParameters"]["userid"]
    name = event["queryStringParameters"]["name"]
    category = event["queryStringParameters"]["category"]
    
    
    #mongo
    db = 'codemarket_devasish'
    client = pymongo.MongoClient('mongodb+srv://sumi:'+urllib.parse.quote('sumi@')+'123@codemarket-staging.k16z7.mongodb.net/'+db+'?retryWrites=true&w=majority')
    database = client[db]
    collection = database['manhattanBeach_scraper']
    
    status = 'Scraping Started'
    query = {'user_id':userid,'name':name}
    document = collection.find_one(query)
    
    if document:
        newvalues = {"$set":{"status":status}}
        collection.update_one(query,newvalues)
        id = document.get("_id")
    else:
        document = {'user_id':userid,
                    'name': name,
                    'status': status,
                    'created by':'UI',
                    'created timestamp': '',
                    'collection of email scraped':''
                }
        
        collection.insert_one(document)
        document = collection.find_one(query)
        id = document.get("_id")

    
    
    #encoding parameters
    category = urllib.parse.quote_plus(category)
    
    
    #vairable definition
    cluster = 'devasish_yelp'
    task_definition = 'devasish_mb_commerce_chamber:31'
    overrides = {"containerOverrides": [{'name':'devasish_mb_commerce_chamber','command':[userid, name, id, category]} ] }
   
    #running fargate task
    result = boto3.client('ecs').run_task(
    cluster = cluster,
    taskDefinition = task_definition,
    overrides = overrides,
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