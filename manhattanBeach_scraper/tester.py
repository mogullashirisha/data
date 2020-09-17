# from mongolia import connect_to_database, authenticate_connection
# from mongolia import DatabaseObject, ID_KEY, DatabaseCollection, 
import urllib

# class TestObject(DatabaseObject):
#     PATH = "codemarket_devasish.manhattanBeach_scraper"
#     DEFAULTS = {'user_id':'devasish','name':'manhattanbeach_scraper'}


# connect_to_database('mongodb+srv://sumi:'+urllib.parse.quote_plus('sumi@123')+'@codemarket-staging.k16z7.mongodb.net/codemarket_devasish?retryWrites=true&w=majority')
# db = 'codemarket_devasish'
# connection_ = 'manhattanBeach_scraper'
# path = db +'.'+connection_

# coll = DatabaseCollection(path)
from mongoengine import *
from mongoengine.context_managers import switch_collection

connect(db = 'codemarket_devasish', host = 'mongodb+srv://sumi:'+urllib.parse.quote_plus('sumi@123')+'@codemarket-staging.k16z7.mongodb.net/codemarket_devasish?retryWrites=true&w=majority')

class Website(EmbeddedDocument):
  business_name = StringField(max_length=250, required=True)
  website_link = StringField(max_length=250, required=True)
  category = StringField(max_length=250, required=True)
  emails = ListField(EmailField())
  telephone = IntField(min_value=0, max_value=9999999999)
  postal_code = IntField()
  region = StringField()
  locality = StringField()
  Address_line2 = StringField()
  street = StringField()
  twitter_links = LineStringField()
  facebook_links = LineStringField()
  state = StringField()
  Address_line1 = StringField()
  city = StringField()

class MB_scraper(Document):
    userid = StringField(max_length=120, required=True)
    name = StringField(max_length=120, required=True)
    status = StringField(max_length=120)
    email_counter = IntField()
    created_timestamp = DateTimeField()
    collection_of_email_scraped = EmbeddedDocumentListField(Website)

with switch_collection(MB_scraper, 'Chamber_of_Commerce') as MB_scraper:

  web1 = Website()
  web1.business_name = "secondtrials"
  web1.website_link = "Edited"
  web1.category = "Edited"
  # web1.emails = ["secondemail@gmail.com","gmail@pasta.com"]

  # scraper1 = MB_scraper()
  # scraper1.user = 'devasish'
  # scraper1.name = 'manhattanbeach_scraper'
  # scraper1.status = 'scrapping'
  # scraper1.email_counter = 1
  target = "5f57a3f6b011042085c43c57"
  new_web = web1
  # MB_scraper.objects(id = target).update(push__collection_of_email_scraped = web1)
  # print(MB_scraper.objects(userid = 'devasish', name = 'hermosa_beach_scraper_test', collection_of_email_scraped__business_name = 'secondtrials').get())
  obj = MB_scraper.objects(userid = 'devasish', name = 'manhattan_scraper', collection_of_email_scraped__business_name = 'Beach Cities Solar Consulting').update(set__collection_of_email_scraped__S__city = "one")
  # scraper1.save()
