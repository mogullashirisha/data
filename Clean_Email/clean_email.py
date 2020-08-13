from validate_email import validate_email
import datetime
import pymongo
import urllib.parse
import argparse

class Cleaner:
    def __init__(self,userid,name):
        self.userid = userid
        self.name = name

    def get_collection(self):
        client = pymongo.MongoClient('mongodb+srv://sumi:'+urllib.parse.quote_plus('sumi@123')+'@codemarket-staging.k16z7.mongodb.net/codemarket_akash?retryWrites=true&w=majority')
        db = client['codemarket_akash']
        collection = db['yelpscrapermailinglist']
        return collection

    def clean(self):
        new_email_collection = set()
        collection = self.get_collection()
        
        query = {'user_id':self.userid,'name': self.name}
        document = collection.find_one(query)
        #print(document['collection of email scraped'])
        email_collection = eval(document['collection of email scraped'])

        print('Cleaning Started')
        for dictionary in email_collection:
            record = eval(dictionary)
            scraped_emails = record['Emails']

            only_valid = set()
            try:
                for email in scraped_emails:
                    if validate_email(email):
                        only_valid.add(email)
            except:
                continue

            if only_valid:
                record['cleaned email'] = only_valid
            else:
                record['cleaned email'] = ''
                
            record['cleaned_timestamp'] = str(datetime.datetime.now())
            record['cleaned_by'] = 'UI'
            
            new_email_collection.add(repr(record))

        print('Cleaning Completed')
        email_collection = repr(new_email_collection)
        print('Updating Database')
        newvalues = { "$set": { 'collection of email scraped': email_collection,'status': 'Cleaning Completed' } }
        collection.update_one(query,newvalues)
        print('Database Updated')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('user_id',type=str,nargs='?',default='123',help='Enter userid')
    parser.add_argument('name',type=str,nargs='?',default='yelpscraper',help='Enter name')
    args = parser.parse_args()

    userid = args.user_id
    name = args.name
    print(userid,name)

    cleaner_obj = Cleaner(userid,name)
    cleaner_obj.clean()

    
        
