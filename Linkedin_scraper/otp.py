import sys
import time
import pymongo
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import urllib.parse
from mongoengine import *
import pandas as pd
from mongoengine.context_managers import switch_collection
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import datetime
import argparse




class linkedin_connection(EmbeddedDocument):
    linkedin = StringField()
    first_name = StringField()
    last_name = StringField()
    status = StringField()
    company = StringField()
    email = StringField()
    website = StringField()
    twitter = StringField()
    education = LineStringField()

class linkedin_scraper(Document):
    userid = StringField()
    connection_details = EmbeddedDocumentListField(linkedin_connection)
    otp = StringField()
    created_timestamp = DateTimeField()
    last_updated = DateTimeField()

def get_scraped_data():
    client = pymongo.MongoClient('mongodb+srv://sumi:'+urllib.parse.quote_plus('sumi@123')+'@codemarket-staging.k16z7.mongodb.net/codemarket_devasish?retryWrites=true&w=majority')
    query={'userid': username}
    db = client["codemarket_devasish"]
    collection = db["LinkedIn"]
    document = collection.find_one(query)
    data = document["connection_details"]
    dataframe = pd.DataFrame(data)
    col = dataframe.linkedin.to_list()
    return col

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('otp',type=str,nargs='?',default=None,help='OTP')
    args = parser.parse_args()
    otp = args.otp
    connect(db = 'codemarket_devasish', host = 'mongodb+srv://sumi:'+urllib.parse.quote_plus('sumi@123')+'@codemarket-staging.k16z7.mongodb.net/codemarket_devasish?retryWrites=true&w=majority')
    with switch_collection(linkedin_scraper, 'LinkedIn') as linkedin_scraper:
      linkedin_scraper.objects(userid = 't7.devasishmahato@gmail.com').update(set__otp = otp)