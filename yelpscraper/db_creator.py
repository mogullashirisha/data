import time
from mongoengine import *
from mongoengine.context_managers import switch_collection
import urllib.parse
import argparse
import datetime
from sys import stdout
import argparse

class Website(EmbeddedDocument):
    business_name = StringField(max_length=250, required=True)
    website_link = StringField(max_length=250, required=True)
    emails = ListField(EmailField(unique= True))
    telephone = IntField(min_value=0)
    postal_code = IntField()
    state = StringField()
    city = StringField()
    keyword = StringField(max_length=250)
    Address_line2 = StringField()
    Address_line1 = StringField()

class MB_scraper(Document):
    userid = StringField(max_length=120, required=True)
    name = StringField(max_length=120, required=True)
    status = StringField(max_length=120)
    city = StringField(max_length=250)
    keywords = ListField(StringField(max_length=250))
    email_counter = IntField()
    limit = IntField()
    created_timestamp = DateTimeField()
    last_updated = DateTimeField()
    collection_of_email_scraped = EmbeddedDocumentListField(Website)

def create_db(userid, name, keyword, city, limit, MB_scraper):
    connect(db = 'codemarket_devasish', host = 'mongodb+srv://sumi:'+urllib.parse.quote_plus('sumi@123')+'@codemarket-staging.k16z7.mongodb.net/codemarket_devasish?retryWrites=true&w=majority')
    with switch_collection(MB_scraper, 'Yelp') as MB_scraper:
        mbscrape = MB_scraper()
        mbscrape.userid = userid
        mbscrape.name = name
        mbscrape.status = "Scraping Started"
        mbscrape.keywords = []
        mbscrape.city = city
        mbscrape.limit = limit
        mbscrape.email_counter = 0
        mbscrape.collection_of_email_scraped = []
        mbscrape.save()
        print(f"{mbscrape.userid},{mbscrape.name},{urllib.parse.quote_plus(mbscrape.keywords)},{urllib.parse.quote_plus(mbscrape.city)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('userid',type=str,nargs='?', default='devasish', help='Enter userid')
    parser.add_argument('name',type=str,nargs='?', default='yelp_scraper', help='Enter name')
    parser.add_argument('keyword',type=str,nargs='?', default=urllib.parse.quote_plus('Realtor'), help='Enter keyword')
    parser.add_argument('city',type=str,nargs='?', default=urllib.parse.quote_plus('Manhattan Beach, CA'), help='Enter city')
    parser.add_argument('limit',type=int,nargs='?', default=24, help='Enter limit')
    args = parser.parse_args()

    userid = args.userid
    name = args.name
    keyword = args.keyword
    city = args.city
    limit = args.limit
    print(userid,name,keyword,city,limit, MB_scraper)

    create_db(userid, name, keyword, city, limit, MB_scraper)