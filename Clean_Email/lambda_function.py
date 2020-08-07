import json
import re
from validate_email import validate_email
import pymongo
import urllib.parse


def lambda_handler(event=None, context=None):    

    mailinglist = 'akash'
    client = pymongo.MongoClient('mongodb+srv://sumi:'+urllib.parse.quote('sumi@')+'123@codemarket-staging.k16z7.mongodb.net/codemarket_akash?retryWrites=true&w=majority')
    db = client.get_database('codemarket_akash')
    collection = db['yelpscrapermailinglist']
    record = collection.find_one({'name':mailinglist})
    
    #status = record['status']
    #print('Last Status',status)
    record['status'] = 'Cleaning'
    status = record['status']
    print('New status',status)
      
    return {
        'statusCode': 200,
        'body': json.dumps(status)
    }

