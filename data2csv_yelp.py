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
  dataframe = dataframe.explode('Address').reset_index(drop=True)
  dataframe.Address = dataframe.Address.apply(lambda x: x.lower() if type(x) == str else np.nan)
  dataframe = dataframe[dataframe['Address'].notna()]
  dataframe.drop_duplicates(inplace = True)
  dataframe = dataframe.reset_index(drop=True)
  return(dataframe)

def export(name = 'yelp_HB'):
    db = 'codemarket_devasish'
    collection = 'Yelp' # 'Chamber_of_Commerce'
    query =  {'userid':'devasish','name':name}
    # columns = ["city"]
    new_col = {"business_name":"Attributes.business_name",
              "website_link": "Attributes.website_link",
              "emails":"Address",
              "keyword": "User.UserAttributes.keyword",
              "telephone": "Attributes.telephone",
              "postal_code": "Location.PostalCode",
              "state": "Location.Region",
              "city": "Location.City",
              "street": "Attributes.address_Line1"
              }
    data = get_data_from_db(db, collection, query, new_col)
    data = convert_to_segment(data)
    data.to_csv(f"exports/yelp_HB.csv",index=False)

if __name__ == "__main__":
  export()