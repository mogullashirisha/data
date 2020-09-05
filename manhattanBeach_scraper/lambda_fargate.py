import json
import boto3
import urllib.parse
import pymongo

def lambda_handler(event, context):
    # TODO implement

    #reading parameters
    userid = event["queryStringParameters"]["user_id"]
    name = event["queryStringParameters"]["name"]
    keyword = event["queryStringParameters"]["keyword"]
    city = event["queryStringParameters"]["city"]
    limit = event["queryStringParameters"]["limit"]
    
    
    #mongo
    db = 'codemarket_devasish'
    client = pymongo.MongoClient('mongodb+srv://sumi:'+urllib.parse.quote('sumi@')+'123@codemarket-staging.k16z7.mongodb.net/'+db+'?retryWrites=true&w=majority')
    database = client[db]
    collection = database['yelpscrapermailinglist']
    
    status = 'Scraping Started'
    query = {'user_id':userid,'name':name}
    document = collection.find_one(query)
    
    if document:
        newvalues = {"$set":{"status":status}}
        collection.update_one(query,newvalues)
    else:
        document = {'user_id':userid,
                    'name': name,
                    'keyword':keyword,
                    'city':city,
                    'limit': limit,
                    'status': status,
                    'created by':'UI',
                    'created timestamp': '',
                    'collection of email scraped':''
                }
        
        collection.insert_one(document)

    
    
    #encoding parameters
    keyword = urllib.parse.quote_plus(keyword)
    city = urllib.parse.quote_plus(city)
    
    
    #vairable definition
    cluster = 'devasish_yelp'
    task_definition = 'devasish_yelp:31'
    overrides = {"containerOverrides": [{'name':'devasish_yelp','command':[userid,name,keyword,city,limit]} ] }
   
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