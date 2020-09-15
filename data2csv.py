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
import numpy as np

def start_database(db_name, collection_name):
  client = pymongo.MongoClient('mongodb+srv://sumi:'+urllib.parse.quote_plus('sumi@123')+'@codemarket-staging.k16z7.mongodb.net/codemarket_devasish?retryWrites=true&w=majority')
  db = client[db_name]
  collection = db[collection_name]
  return collection

def get_data_from_db(db_name, collection_name, query, new_col, columns = None):
  collection = start_database(db_name, collection_name)
  document = collection.find_one(query)
  data_email = document["collection_of_email_scraped"]
  dataframe = pandas.DataFrame(data_email)
  dataframe = dataframe.rename(columns = new_col)
  m,_ = dataframe.shape
  if columns != None:
    for col in columns:
      dataframe["Attributes." + col] = [document[col]]*m
  return(dataframe)

def convert_to_segment(dataframe):
  m,_ = dataframe.shape
  dataframe['ChannelType'] = ["EMAIL"]*m
  dataframe = dataframe.explode('EMAIL').reset_index(drop=True)
  dataframe.EMAIL = dataframe.EMAIL.apply(lambda x: x.lower() if type(x) == str else np.nan)
  dataframe.dropna(inplace= True)
  # dataframe.drop_duplicates(inplace = True)
  dataframe = dataframe.reset_index(drop=True)
  return(dataframe)

def export(name = 'mb_lawyer'):
    db = 'codemarket_devasish'
    collection = 'Chamber_of_Commerce' #'yelpscrapermailinglist'
    query =  {'user_id':'devasish','name':name}
    columns = ["city"]#"keyword", 
    new_col = {"business_name":"Attributes.business_name",
              "website_link": "Attributes.website_link",
              "emails":"EMAIL",
              "category": "Attributes.category",
              "telephone": "Attributes.telephone",
              "postal_code": "Attributes.postal_code",
              "region": "Attributes.region",
              "street": "Attributes.street",
              "locality": "Attributes.locality",
              }
    data = get_data_from_db(db, collection, query, new_col , columns=columns)
    data = convert_to_segment(data)
    print(data)
    data.to_csv(f"exports/Chamber of Commerce-HB-Advertising and Media.csv")

if __name__ == "__main__":
  '''ls = ["MB_Realtor",
        "mb_real_estate",
        "mb_lawyer",
        "mb_legal",
        "mb_accountant",
        "mb_architect",
        "mb_marketing",
        "mb_advertisement",
        "mb_photographer",
        "mb_therapist",
        "mb_software",
        "mb_insurance",
        "mb_financial",
        "mb_consultant",
        "mb_nutritionist"]
  for name in ls:'''
  export("hermosa_beach_scraper")