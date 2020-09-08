#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pymongo
import urllib
import pandas as pd
import dns


# In[1]:


def db():
    client = pymongo.MongoClient('mongodb+srv://sumi:'+urllib.parse.quote_plus('sumi@123')+'@codemarket-staging.k16z7.mongodb.net/codemarket_aryman?retryWrites=true&w=majority')
    db = client['codemarket_aryman']
    collection = db['Instagram']


    # In[4]:


    df = pd.read_csv('filtered.csv')
    df.shape
    _id = 1
    l =[]
    for i in range(len(df)):
        d = dict()
        name = df.iloc[i,0]
        insta_link = df.iloc[i,1]
        website = df.iloc[i,2]
        email = df.iloc[i,3]
        d = {'_id':_id,
            'Business_Name':name,
             'Insta_link':insta_link,
            'Website':website,
            'Email':email}
        print(name,insta_link,website,email)
        l.append(d)
        #entry = collection.insert_one(d)
        _id +=1
    x = collection.insert_many(l)


# In[ ]:




