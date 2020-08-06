from validate_email import validate_email
import datetime
import pymongo
import urllib.parse
import argparse

class Cleaner:
    def __init__(self,mailinglist):
        self.mailinglist = mailinglist

    def get_collection(self):
        client = pymongo.MongoClient('mongodb+srv://sumi:'+urllib.parse.quote_plus('sumi@123')+'@codemarket-staging.k16z7.mongodb.net/codemarket_akash?retryWrites=true&w=majority')
        db = client['codemarket_akash']
        collection = db['yelpscrapermailinglist']
        return collection

    def clean(self):
        new_email_collection = set()
        collection = self.get_collection()
        
        query = {'name': self.mailinglist}
        document = collection.find_one(query)
        #print(document['collection of email scraped'])
        email_collection = eval(document['collection of email scraped'])
        print(email_collection)
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
            print(datetime.datetime.now())
            new_email_collection.add(repr(record))

        email_collection = repr(new_email_collection)

        print()
        print(email_collection)
        newvalues = { "$set": { 'collection of email scraped': email_collection,'status': 'Completed' } }
        collection.update_one(query,newvalues)
        

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('mailinglist',type=str,nargs='?',default='akash',help='Enter mailinglist')
    args = parser.parse_args()

    mailinglist = args.mailinglist
    print(mailinglist)

    cleaner_obj = Cleaner(mailinglist)
    cleaner_obj.clean()

    
        
