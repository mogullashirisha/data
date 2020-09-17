from requests import get
from bs4 import BeautifulSoup
import re
import os
import time
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
import pymongo
from mongoengine import *
from mongoengine.context_managers import switch_collection
import urllib.parse
import argparse
import datetime
from sys import stdout
import pandas as pd
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

class MB_scraper(Document):
    userid = StringField(max_length=120, required=True)
    name = StringField(max_length=120, required=True)
    chamber_of_commerce = StringField(max_length=250, required=True)
    status = StringField(max_length=120)
    email_counter = IntField()
    created_timestamp = DateTimeField()
    last_updated = DateTimeField()
    collection_of_email_scraped = ListField(EmbeddedDocumentField(Website))

class Scraper:
    def __init__(self, userid, name, id, category):
        self.userid = userid
        self.name = name
        self.category = category
        self.id = id
        self.AllInternalLinks = set()
        self.AllInternalEmails = set()
        self.AllEmails = {}
        self.final_result = set()
        self.counter = 0
        self.email_counter = 0
        self.website_scrapped = {}
        self.all_websites = []
        try:
            self.get_scraped_data()
        except:
            print("No old data")

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
        self.all_websites = col

    def scrape(self, MB_scraper):
        self.flag = 0
        self.no_email = True
        print('Begin Scraping')
        connect(db = 'codemarket_devasish', host = 'mongodb+srv://sumi:'+urllib.parse.quote_plus('sumi@123')+'@codemarket-staging.k16z7.mongodb.net/codemarket_devasish?retryWrites=true&w=majority')
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument('--disable-dev-shm-usage')
        self.driver = webdriver.Chrome('/usr/local/bin/chromedriver',chrome_options=chrome_options)
        # self.driver = webdriver.Chrome('E:/Codes/chromedriver.exe',chrome_options=chrome_options)

        with switch_collection(MB_scraper, 'manhattanBeach_scraper') as MB_scraper:
            url = "https://business.manhattanbeachchamber.com/members"
            # print(url)
            self.driver.get(url)
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'html.parser')
            bussiness_list = soup.find("div", id="gz-ql")
            lilist = bussiness_list.find('ul').findChildren(['li'])
            for li in lilist:
                cate = li.findChildren(['a'])
                status = f'Scraping website'
                MB_scraper.objects(userid = self.userid, name = self.name).update(set__status = status )
                category = cate[0].text
                if urllib.parse.quote_plus(category) != self.category:
                    continue
                category_url = cate[0]['href']
                self.driver.get(category_url)
                html = self.driver.page_source
                soup = BeautifulSoup(html, 'html.parser')
                business_list = soup.findAll('div',class_="card-header")
                self.website_scrapped[category] = len(business_list)
                for li in business_list:
                    self.email_counter = 0
                    business = li.find('a')
                    business_name = business['alt']
                    print(business_name)
                    profilelink = business['href']
                    self.driver.get(profilelink)
                    time.sleep(7)
                    profile = self.driver.page_source
                    profile_soup = BeautifulSoup(profile, 'html.parser')
                    try:
                        number = profile_soup.find('span', itemprop = "telephone").text
                        telephone = self.get_telephone_no(number)
                    except:
                        telephone = 0
                    try:
                        postal_code = profile_soup.find('span', itemprop = "postalCode").text
                    except:
                        postal_code = 0
                    try:
                        region = profile_soup.find('span', itemprop = "addressRegion").text
                    except:
                        region = " "
                    try:
                        locality = profile_soup.find('span', itemprop = "addressLocality").text
                    except:
                        locality = " "    
                    try:
                        street = profile_soup.find('span', itemprop = "streetAddress").text
                    except:
                        street = " "
                    website = profile_soup.find('a', itemprop = "url")
                    try:
                        websitelink = website['href']
                    except:
                        print(f"{business_name}-->Website Not Available")
                        continue
                
                    if business_name in self.all_websites:
                        print("Website data already Available")
                        if telephone != 0:
                            MB_scraper.objects(userid = self.userid, name = self.name, collection_of_email_scraped__business_name = business_name).update(set__collection_of_email_scraped__S__telephone = telephone)
                        if postal_code != 0:
                            MB_scraper.objects(userid = self.userid, name = self.name, collection_of_email_scraped__business_name = business_name).update(set__collection_of_email_scraped__S__postal_code = postal_code)
                        if street != ' ':
                            MB_scraper.objects(userid = self.userid, name = self.name, collection_of_email_scraped__business_name = business_name).update(set__collection_of_email_scraped__S__Address_line1 = street)
                        if region != ' ':
                            MB_scraper.objects(userid = self.userid, name = self.name, collection_of_email_scraped__business_name = business_name).update(set__collection_of_email_scraped__S__state = region)
                        if locality != ' ':
                            MB_scraper.objects(userid = self.userid, name = self.name, collection_of_email_scraped__business_name = business_name).update(set__collection_of_email_scraped__S__city = locality)

                    else:
                        self.all_websites.append(business_name)
                        self.driver.get(websitelink)
                        websitepage = self.driver.page_source
                        websiteSoup = BeautifulSoup(websitepage, 'html.parser')
                        new_emails = set(re.findall(r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.[a-z0-9\.\-+_]+", websitepage, re.I))
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
                            print("\n------VALID EMAIL SET------")
                            print(only_valid)
                        self.AllInternalEmails.update(only_valid)
                        self.getInternalLinks(websiteSoup, self.splitaddress(websitelink)[0])
                        self.AllInternalLinks.clear()

                        website_object = Website()
                        website_object.business_name = business_name
                        website_object.website_link = websitelink
                        website_object.category = category   
                        website_object.emails = list(self.AllInternalEmails)
                        if telephone != 0:
                            website_object.telephone = telephone
                        if postal_code != 0:
                            website_object.postal_code = postal_code
                        if street != ' ':
                            website_object.Address_line1 = street
                        if region != ' ':
                            website_object.state = region
                        if locality != ' ':
                            website_object.city = locality
                
                        self.AllInternalEmails.clear()
                        
                        try:
                            MB_scraper.objects(userid = self.userid, name = self.name).update(push__collection_of_email_scraped = website_object)
                            MB_scraper.objects(userid = self.userid, name = self.name).update(inc__email_counter = self.email_counter)
                            MB_scraper.objects(userid = self.userid, name = self.name).update(set__last_updated = datetime.datetime.now())
                        except:
                            print("Insertion Failed --- Data Not Unique")
        
            MB_scraper.objects(userid = self.userid, name = self.name).update(set__status = "Scraping Completed")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('user_id', type=str, nargs='?', default = 'devasish', help='Enter userid')
    parser.add_argument('name', type=str, nargs='?', default = 'manhattanbeach_scraper', help='Enter name')
    parser.add_argument('id', type=str, nargs='?', default = "5f57a3f6b011042085c43c57", help = "Object Id")
    parser.add_argument('category', type=str, nargs='?', default = urllib.parse.quote_plus("Automotive & Marine"), help='Enter limit')
    args = parser.parse_args()

    userid = args.user_id
    name = args.name
    id = args.id
    category = args.category
    print(userid, name, category)

    scraper_obj = Scraper(userid, name, id, category)
    scraper_obj.scrape(MB_scraper)