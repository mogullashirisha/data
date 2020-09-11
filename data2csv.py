from requests import get
from bs4 import BeautifulSoup
import re
import os
import time
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
import pymongo
import urllib.parse
import argparse
import datetime
import pandas
from ast import literal_eval

def start_database():
  client = pymongo.MongoClient('mongodb+srv://sumi:'+urllib.parse.quote_plus('sumi@123')+'@codemarket-staging.k16z7.mongodb.net/codemarket_devasish?retryWrites=true&w=majority')
  db = client['codemarket_devasish']
  collection = db['manhattanBeach_scraper']
  return collection

def get_data_from_db():
  collection = start_database()
  query = {'user':'devasish','name':'manhattanbeach_scraper'}
  document = collection.find_one(query)
  data_email = document["collection_of_email_scraped"]
  dataframe = pandas.DataFrame(data_email)
  return(dataframe)

def convert_to_segment(dataframe):
  new_col = {"business_name":"Attributes.business_name", "website_link": "Attributes.website_link", "category": "Attributes.category", "emails":"EMAIL"}
  dataframe = dataframe.rename(columns = new_col)
  m,_ = dataframe.shape
  dataframe['ChannelType'] = ["EMAIL"]*m
  return(dataframe)

data = get_data_from_db()
data = convert_to_segment(data)
data.to_csv("exports/manhattan_data_complete.csv")
