from requests import get
from bs4 import BeautifulSoup
import re
import os
import time
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
import pymongo
import pandas as pd
from mongoengine import *
from mongoengine.context_managers import switch_collection
import urllib.parse
import argparse
import datetime
from sys import stdout
from validate_email import validate_email

class Website(EmbeddedDocument):
    business_name = StringField(max_length=250, required=True)
    website_link = StringField(max_length=250, required=True)
    category = StringField(max_length=250, required=True)
    emails = ListField(EmailField())
    telephone = IntField(min_value=0, max_value=9999999999)
    postal_code = IntField()
    state = StringField()
    city = StringField()
    Address_line2 = StringField()
    Address_line1 = StringField()
    # facebook_links = ListField(StringField())
    # twitter_links = ListField(StringField())

class MB_scraper(Document):
    userid = StringField(max_length=120, required=True)
    name = StringField(max_length=120, required=True)
    chamber_of_commerce = StringField(max_length=250, required=True)
    status = StringField(max_length=120)
    email_counter = IntField()
    created_timestamp = DateTimeField()
    last_updated = DateTimeField()
    collection_of_email_scraped = ListField(EmbeddedDocumentField(Website))

# class prev_data(HB_scraper):

class Scraper:
    def __init__(self, userid, name, id):
        self.userid = userid
        self.name = name
        self.id = id
        self.AllInternalLinks = set()
        self.AllInternalEmails = set()
        self.AllEmails = {}
        self.final_result = set()
        self.counter = 0
        self.email_counter = 0
        self.all_business = []
        try:
            self.get_scraped_data()
        except:
            print("NO data")

    def getInternalLinks(self,bsobj, includeurl):
        internalLinks = []
        for links in bsobj.findAll("a", {"href": re.compile("^(/|.*" + includeurl + ")")}):
            if links.attrs['href'] is not None:
                if links.attrs["href"] not in internalLinks:
                    internalLinks.append(links.attrs['href'])
        
        for link in internalLinks:
             truncURL = link.replace("http://", "").replace("https://", "").replace(includeurl, "")
             sep = "/"
             spliturl = truncURL.split(sep,2)
             if len(spliturl)>=2:
                 truncURL = spliturl[1] 
                 removeParameterURL = spliturl[1].split("?", 1)
                 if len(removeParameterURL) >=1:
                     truncURL = removeParameterURL[0]
                 else:
                     truncURL = ""
             if truncURL not in self.AllInternalLinks:
                 if link!= "http://"+includeurl and link != "https://"+includeurl and link != '/' and link != "http://"+includeurl+'/' and link != "https://"+includeurl+'/':
                    self.AllInternalLinks.add(truncURL)
                    try:
                        websitepage = get("http://"+includeurl+"/" + truncURL)
                        stdout.write(f"\r{includeurl}/{truncURL}")
                    except:
                        continue
                    
                    # facebook
                    # facebook
                    # facebook_links = set(re.findall("https?://(www\\.)?twitter\\.com/[^(share)]?(\\w+\\.?)+", websitepage.text, re.I))
                    # self.AllFBlinks.update(facebook_links)
                    
                    # # twitter
                    # twitter_links = set(re.findall("https?://(www\\.)?twitter\\.com/[^(share)]?(\\w+\\.?)+", websitepage.text, re.I))
                    # self.AllTwitterlinks.update(twitter_links)
                    

                    new_emails = set(re.findall(r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.[a-z0-9\.\-+_]+", websitepage.text, re.I))
                    only_valid = set()
                    for em in new_emails:
                        try:
                            self.AllEmails[em] += 1
                        except KeyError:
                            if validate_email(em):
                                only_valid.add(em)
                            self.AllEmails[em] = 1
                    self.email_counter += len(only_valid)
                    if len(only_valid) > 0:
                        print("------VALID EMAIL SET------")
                        print(only_valid)
                    self.AllInternalEmails.update(only_valid)
                    websitepage_soup = BeautifulSoup(websitepage.text, 'html.parser')
                    self.getInternalLinks(websitepage_soup, includeurl)
        return (internalLinks)

    def splitaddress(self,address):
        return (address.replace("http://", "").replace("https://", "").split("/"))

    def get_telephone_no(self, telephone):
        return int(re.sub(r'[^\w]', '', telephone))

    def get_scraped_data(self):
        client = pymongo.MongoClient('mongodb+srv://sumi:'+urllib.parse.quote_plus('sumi@123')+'@codemarket-staging.k16z7.mongodb.net/codemarket_devasish?retryWrites=true&w=majority')
        query={'userid': self.userid,'name': self.name}
        db = client["codemarket_devasish"]
        collection = db["Chamber_of_Commerce"]
        document = collection.find_one(query)
        data_email = document["collection_of_email_scraped"]
        dataframe = pd.DataFrame(data_email)
        col = dataframe.business_name.to_list()
        self.all_business = col

    def scrape(self, MB_scraper):
        self.flag = 0
        self.no_email = True
        print('Begin Scraping')
        connect(db = 'codemarket_devasish', host = 'mongodb+srv://sumi:'+urllib.parse.quote_plus('sumi@123')+'@codemarket-staging.k16z7.mongodb.net/codemarket_devasish?retryWrites=true&w=majority')
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-gpu")
        # chrome_options.add_argument('--disable-dev-shm-usage')
        # self.driver = webdriver.Chrome('/usr/local/bin/chromedriver',chrome_options=chrome_options)
        self.driver = webdriver.Chrome('E:/Codes/chromedriver.exe',chrome_options=chrome_options)

        with switch_collection(MB_scraper, 'Chamber_of_Commerce') as MB_scraper:
            print("Connection Established")
            url = "https://id21265.securedata.net/elsegund/autowebsite/cw_lst.htm"
            # print(url)
            self.driver.get(url)
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'html.parser')
            lilist = soup.findAll("li")
            for li in lilist:
                cate = li.findChildren(['a'])
                status = f'Scraping website'
                MB_scraper.objects(id = self.id).update(set__status = status)
                category = cate[0].text
                category_url = cate[0]['href']
                self.driver.get( "https://id21265.securedata.net/elsegund/autowebsite/" + category_url)
                html = self.driver.page_source
                soup = BeautifulSoup(html, 'html.parser')
                business_list = soup.find("h2").findNextSiblings('p')
                for li in business_list:
                    self.email_counter = 0
                    business = li.find('a')
                    business_name = business.text
                    if business_name in self.all_business:
                        print("already in data")
                        continue
                    print(business_name)
                    websitelink = business['href']
                    para = li.text.strip().split('\n')
                    telephone = self.get_telephone_no(para[3][:13])
                    address_line1 = para[1]
                    address = para[2].split()
                    postal_code = int(address.pop(-1))
                    state = address.pop(-1)
                    city = ' '.join(address)
                    
                    self.driver.get(websitelink)
                    websitepage = self.driver.page_source
                    websiteSoup = BeautifulSoup(websitepage, 'html.parser')
                    
                    new_emails = set(re.findall(r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.[a-z0-9\.\-+_]+", websitepage, re.I))
                    only_valid = set()
                    for em in new_emails:
                        if validate_email(em):
                            only_valid.add(em.lower())
                    if len(only_valid) > 0:
                        print("------VALID EMAIL SET------")
                        print(only_valid)
                    self.AllInternalEmails.update(only_valid)
                    self.getInternalLinks(websiteSoup, self.splitaddress(websitelink)[0])
                    self.email_counter += len(self.AllInternalEmails)
                    self.AllInternalLinks.clear()
            
                    website_object = Website()
                    website_object.business_name = business_name
                    website_object.website_link = websitelink
                    website_object.category = category
                    website_object.emails = list(self.AllInternalEmails)
                    website_object.telephone = telephone
                    website_object.postal_code = postal_code
                    website_object.Address_line1 = address_line1
                    website_object.state = state
                    website_object.city = city
            
                    self.AllInternalEmails.clear()
                    
                    try:
                        MB_scraper.objects(id = self.id).update(push__collection_of_email_scraped = website_object)
                        MB_scraper.objects(id = self.id).update(inc__email_counter = self.email_counter)
                        MB_scraper.objects(id = self.id).update(set__last_updated = datetime.datetime.now())
                    except:
                        print("Insertion Failed --- Data Not Unique")
        
            MB_scraper.objects(id = self.id).update(set__status = "Scraping Completed")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('user_id', type=str, nargs='?', default = 'devasish', help='Enter userid')
    parser.add_argument('name', type=str, nargs='?', default = 'El_Segundo_scraper', help='Enter name')
    parser.add_argument('id', type=str, nargs='?', default = "5f6dad77a177df5818aa31d8", help = "Object Id")
    args = parser.parse_args()

    userid = args.user_id
    name = args.name
    id = args.id
    print(userid, name)

    scraper_obj = Scraper(userid, name, id)
    scraper_obj.scrape(MB_scraper)