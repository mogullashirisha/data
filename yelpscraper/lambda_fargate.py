#please rename this file as lambda_function.py while uploading to lambda
import json
import boto3
import urllib.parse

def lambda_handler(event, context):
    # TODO implement
    print(event)
    
   
    keyword = event["params"]["querystring"]["keyword"]
    city = event["params"]["querystring"]["city"]
    limit = event["params"]["querystring"]["limit"]
    
    keyword = urllib.parse.quote_plus(keyword)
    city = urllib.parse.quote_plus(city)
    limit = urllib.parse.quote_plus(limit)
    
    cluster = 'yelpscraper'
    task_definition = 'yelpscraper:10'
    overrides = {"containerOverrides": [{'name':'docker_ec2','command':[keyword,city,limit]} ] }
    

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
   
    print(keyword,city)
    return {
        'statusCode': 200,
        'body': json.dumps(str(result))
    }
  

