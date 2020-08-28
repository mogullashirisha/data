import json
import boto3
import urllib.parse

def lambda_handler(event, context):
    # TODO implement

    #reading parameters
    keyword = event["queryStringParameters"]["keyword"]
    city = event["queryStringParameters"]["city"]
    limit = event["queryStringParameters"]["limit"]
    
    #encoding parameters
    keyword = urllib.parse.quote_plus(keyword)
    city = urllib.parse.quote_plus(city)
    
    #vairable definition
    cluster = 'yelpscraper'
    task_definition = 'yelpscraper:12'
    overrides = {"containerOverrides": [{'name':'docker_ec2','command':[keyword,city,limit]} ] }
   
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