import re
import os
import pymongo
import pandas
import numpy as np
import urllib.parse
import argparse

def start_database(db_name, collection_name):
  client = pymongo.MongoClient('mongodb+srv://sumi:'+urllib.parse.quote_plus('sumi@123')+'@codemarket-staging.k16z7.mongodb.net/codemarket_devasish?retryWrites=true&w=majority')
  db = client[db_name]
  collection = db[collection_name]
  return collection

def get_data_from_db(db_name, collection_name, query, new_col, columns = None):
  collection = start_database(db_name, collection_name)
  document = collection.find_one(query)
  data_email = document["connection_details"]
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
  dataframe.drop_duplicates(inplace = True)
  dataframe = dataframe.reset_index(drop=True)
  return(dataframe)

def export(username):
    db = 'codemarket_devasish'
    collection = 'LinkedIn'
    query =  {'userid':username}
    new_col = {"first_name":"User.UserAttributes.FirstName",
              "last_name":"User.UserAttributes.LastName",
              "email":"Address",
              "company": "User.UserAttributes.Company",
              "linkedin": "User.UserAttributes.LinkedIn_Profile",
              "website": "User.UserAttributes.Website",
              "twitter": "User.UserAttributes.Twitter"
              }
    df = get_data_from_db(db, collection, query, new_col)
    df = convert_to_segment(df)
    df.to_csv(f"LinkedIn.csv",index=False)

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument('username', type=str, nargs='?', default = 'mysumifoods@gmail.com', help='Enter userid')
  args = parser.parse_args()

  username = args.username
  export(username)