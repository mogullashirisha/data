import time
from mongoengine import *
from mongoengine.context_managers import switch_collection
import urllib.parse
import argparse
import datetime
from sys import stdout

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
    user_id = StringField(max_length=120, required=True)
    name = StringField(max_length=120, required=True)
    status = StringField(max_length=120)
    city = StringField(max_length=250)
    keywords = ListField(StringField(max_length=250))
    email_counter = IntField()
    limit = IntField()
    created_timestamp = DateTimeField()
    last_updated = DateTimeField()
    collection_of_email_scraped = EmbeddedDocumentListField(Website)

connect(db = 'codemarket_devasish', host = 'mongodb+srv://sumi:'+urllib.parse.quote_plus('sumi@123')+'@codemarket-staging.k16z7.mongodb.net/codemarket_devasish?retryWrites=true&w=majority')
with switch_collection(MB_scraper, 'Yelp') as MB_scraper:
    mbscrape = MB_scraper()
    mbscrape.user_id = "devasish"
    mbscrape.name = "yelp_scraper"
    mbscrape.status = "Scraping Started"
    mbscrape.keywords = []
    mbscrape.city = "Manhattan Beach, CA"
    mbscrape.limit = 24
    mbscrape.email_counter = 0
    mbscrape.collection_of_email_scraped = []
    mbscrape.save()
    print(f"{mbscrape.user_id},{mbscrape.name},{urllib.parse.quote_plus(mbscrape.keywords)},{urllib.parse.quote_plus(mbscrape.city)},10")