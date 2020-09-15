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
    category = StringField(max_length=250, required=True)
    emails = ListField(EmailField())
    telephone = IntField(min_value=0, max_value=9999999999)
    state = StringField()
    Address_line1 = StringField()
    Address_line2 = StringField()
    city = StringField()
    postal_code = IntField()
    # facebook_links = ListField(StringField())
    # twitter_links = ListField(StringField())

class MB_scraper(Document):
    userid = StringField(max_length=120, required=True)
    name = StringField(max_length=120, required=True)
    chamber_of_commerce = StringField(max_length=250, required=True)
    status = StringField(max_length=120)
    email_counter = IntField()
    created_timestamp = DateTimeField(default = datetime.datetime.now())
    last_updated = DateTimeField()
    collection_of_email_scraped = ListField(EmbeddedDocumentField(Website))

connect(db = 'codemarket_devasish', host = 'mongodb+srv://sumi:'+urllib.parse.quote_plus('sumi@123')+'@codemarket-staging.k16z7.mongodb.net/codemarket_devasish?retryWrites=true&w=majority')
with switch_collection(MB_scraper, 'Chamber_of_Commerce') as MB_scraper:
    mbscrape = MB_scraper()
    mbscrape.userid = "devasish"
    mbscrape.name = "hermosa_beach_scraper"
    mbscrape.status = "Scraping Started"
    mbscrape.chamber_of_commerce = "Hermosa Beach"
    mbscrape.email_counter = 0
    mbscrape.collection_of_email_scraped = []
    mbscrape.save()
    print(f"{mbscrape.userid},{mbscrape.name}")