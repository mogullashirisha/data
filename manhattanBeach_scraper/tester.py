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

class MB_scraper(Document):
    user = StringField(max_length=120, required=True)
    name = StringField(max_length=120, required=True)
    status = StringField(max_length=120)
    email_counter = IntField()
    created_timestamp = DateTimeField()
    collection_of_email_scraped = EmbeddedDocumentListField(Website)

with switch_collection(MB_scraper, 'manhattanBeach_scraper') as MB_scraper:

  web1 = Website()
  web1.business_name = "secondtrials"
  web1.website_link = "seconftrial.com"
  web1.category = "second_Category"
  web1.emails = ["secondemail@gmail.com","gmail@pasta.com"]

  # scraper1 = MB_scraper()
  # scraper1.user = 'devasish'
  # scraper1.name = 'manhattanbeach_scraper'
  # scraper1.status = 'scrapping'
  # scraper1.email_counter = 1
  target = "5f57a3f6b011042085c43c57"
  new_web = web1
  # MB_scraper.objects(id = target).update(push__collection_of_email_scraped = web1)
  MB_scraper.objects(id = target).update(set__email_counter = 1 )
  # scraper1.save()
