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
    emails = ListField(EmailField(unique= True))
    telephone = IntField(min_value=0)
    postal_code = IntField()
    state = StringField()
    city = StringField()
    keyword = StringField(max_length=250)
    Address_line2 = StringField()
    Address_line1 = StringField()

class MB_scraper(Document):
    user_id = StringField(max_length=120, required=True)
    name = StringField(max_length=120, required=True)
    status = StringField(max_length=120)
    city = StringField(max_length=250)
    keywords = ListField(StringField(max_length=250))
    email_counter = IntField()
    limit = IntField()
    created_timestamp = DateTimeField()
    last_updated = DateTimeField()
    collection_of_email_scraped = EmbeddedDocumentListField(Website)

class Scraper:
    def __init__(self,userid,name,keyword,city,limit):
        self.userid = userid
        self.name = name
        self.keyword = keyword
        self.collections = "Yelp"
        self.city = city
        self.limit = limit
        self.counter = 0
        self.AllInternalLinks = set()
        self.AllInternalEmails = set()
        self.all_websites = []
        self.final_result = set()
        self.email_counter = 0
        try:
            self.get_scraped_data()
        except:
            print("data not available")

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
                 if link != "http://"+includeurl and link != "https://"+includeurl and link != '/' and link != "http://"+includeurl+'/' and link != "https://"+includeurl+'/':
                    self.AllInternalLinks.add(truncURL)
                    try:
                        websitepage = get("http://"+includeurl+"/" + truncURL)
                    except:
                        continue
                    new_emails = set(re.findall(r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.[a-z0-9\.\-+_]+", websitepage.text, re.I))
                    only_valid = set()
                    for em in new_emails:
                        if validate_email(em):
                            only_valid.add(em)
                    self.email_counter += len(only_valid)
                    print("------VALID EMAIL SET------")
                    print(only_valid)
                    self.AllInternalEmails.update(only_valid)
                    websitepage_soup = BeautifulSoup(websitepage.text, 'html.parser')
                    self.getInternalLinks(websitepage_soup,includeurl)
        return (internalLinks)

    def get_scraped_data(self):
        client = pymongo.MongoClient('mongodb+srv://sumi:'+urllib.parse.quote_plus('sumi@123')+'@codemarket-staging.k16z7.mongodb.net/codemarket_devasish?retryWrites=true&w=majority')
        query={'user_id': self.userid,'name': self.name}
        db = client["codemarket_devasish"]
        collection = db[self.collections]
        document = collection.find_one(query)
        data_email = document["collection_of_email_scraped"]
        dataframe = pd.DataFrame(data_email)
        col = dataframe.business_name.to_list()
        self.all_websites = col

    def splitaddress(self,address):
        return (address.replace("http://", "").replace("https://", "").split("/"))

    def get_telephone_no(self, telephone):
        return int(re.sub(r'[^\w]', '', telephone))

    def get_url(self, start=0):
        # https://www.yelp.com/search?find_desc=Therapist&find_loc=San+Francisco%2C+CA&ns=
        desc = self.keyword
        loc = self.city
        url = f'https://www.yelp.com/search?find_desc={desc}&find_loc={loc}&ns=1&start={start}'
        return url

    def scrape(self, MB_scraper):
        self.flag = 0
        self.no_email = True
        print('Begin Scraping')
        connect(db = 'codemarket_devasish', host = 'mongodb+srv://sumi:'+urllib.parse.quote_plus('sumi@123')+'@codemarket-staging.k16z7.mongodb.net/codemarket_devasish?retryWrites=true&w=majority')
        while self.no_email and self.flag < 50:
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument('--disable-dev-shm-usage')
            driver = webdriver.Chrome('/usr/local/bin/chromedriver',chrome_options=chrome_options)
            # driver = webdriver.Chrome('E:/Codes/chromedriver.exe')#,chrome_options=chrome_options)
            try:
                with switch_collection(MB_scraper, self.collections) as MB_scraper:
                    if self.flag == 0:
                        MB_scraper.objects(user_id = self.userid, name = self.name).update(push__keywords = self.keyword)
                        MB_scraper.objects(user_id = self.userid, name = self.name).update(set__limit = self.limit)
                    for start in range(0, self.limit * 10 , 10):
                        url = self.get_url(start)
                        # print(url)
                        driver.get(url)
                        html = driver.page_source
                        soup = BeautifulSoup(html, 'html.parser')
                        # bussiness_list = soup.find('ul',class_="lemon--ul__373c0__1_cxs undefined list__373c0__2G8oH")
                        # bussiness_list = soup.find('ul',class_="lemon--ul__09f24__1_cxs undefined list__09f24__OXDHW")
                        bussiness_list = soup.find('ul',class_="lemon--ul__09f24__1_cxs undefined list__09f24__17TsU")
                        lilist = bussiness_list.findChildren(['li'])
                        for li in lilist:
                            status = 'Scraping website'
                            self.email_counter = 0
                            MB_scraper.objects(user_id = self.userid, name = self.name).update(set__status = status)
                            # link = li.find('a',class_='lemon--a__373c0__IEZFH link__373c0__1G70M link-color--inherit__373c0__3dzpk link-size--inherit__373c0__1VFlE')
                            # link = li.find('a',class_='lemon--a__373c0__IEZFH link__373c0__1UGBs photo-box-link__373c0__1AMDk link-color--blue-dark__373c0__12C_y link-size--default__373c0__3m55w')
                            link = li.find('a',class_="lemon--a__09f24__IEZFH link__09f24__1kwXV link-color--inherit__09f24__3PYlA link-size--inherit__09f24__2Uj95")
                            if link == None:
                                continue
                        
                            driver.get("https://www.yelp.com/" + link['href'])
                            time.sleep(7)
                            profile = driver.page_source
                            profile_soup = BeautifulSoup(profile, 'html.parser')
                            websitelink = None
                            business_name = profile_soup.find('h1',class_ = "lemon--h1__373c0__2ZHSL heading--h1__373c0___56D3 undefined heading--inline__373c0__1jeAh").text
                            
                            address_line2, street, city, state = ' '*4
                            postal_code, telephone = 0, 0

                            get_direction = profile_soup.find("a", string = "Get Directions")

                            if profile_soup.find("p", string="Phone number") != None:
                                if profile_soup.find("p", string="Phone number").findNext('p') != None:
                                        telephone = self.get_telephone_no(profile_soup.find("p", string="Phone number").findNext('p').text)

                            if profile_soup.find("p", string="Business website") != None:
                                if profile_soup.find("p", string="Business website").findNext('p') != None:
                                    if profile_soup.find("p", string="Business website").findNext('p').find('a') != None:
                                        websitelink = profile_soup.find("p", string="Business website").findNext('p').find('a')

                            if websitelink == None:
                                print("Link Not Found")
                                print("https://www.yelp.com/" + link['href'])
                                continue
                            
                            if business_name == None:
                                print("NO business Name")
                                business_name = websitelink
                            print(business_name)
                            
                            if get_direction == None:
                                print("No direction")
                            else:
                                try:
                                    get_direction_link = "https://www.yelp.com/" + get_direction["href"]
                                    driver.get(get_direction_link)
                                    address_page = driver.page_source
                                    address_soup = BeautifulSoup(address_page, 'html.parser')
                                    address = address_soup.find("address").text
                                    address = address.replace('\n', '')
                                    address_comp_list = address.split(', ')
                                    ls_len = len(address_comp_list)
                                    if ls_len == 4:
                                        address_line2, street, city, state = address_comp_list
                                    elif ls_len == 3:
                                        street, city, state = address_comp_list
                                    elif ls_len == 2:
                                        city, state = address_comp_list
                                    elif ls_len == 1:
                                        state = address_comp_list[0]
                                    elif ls_len > 4:
                                        street, city, state = address_comp_list[-3:]
                                        address_line2 = ', '.join(address_comp_list)
                                    address_line2 = address_line2.strip()
                                    street = street.strip()
                                    city = city.strip()
                                    state = state.strip()

                                    if state != '':
                                        try:
                                            state, postal_code = state.split(' ')
                                            postal_code = int(postal_code)
                                        except:
                                            print("Problem in Postal Code")
                                except:
                                    print("Unable to get Direction")

                            if business_name in self.all_websites:
                                print("Website data already Available")
                                if telephone != 0:
                                    MB_scraper.objects(userid = self.userid, name = self.name, collection_of_email_scraped__business_name = business_name).update(set__collection_of_email_scraped__S__telephone = telephone)
                                if postal_code != 0:
                                    MB_scraper.objects(userid = self.userid, name = self.name, collection_of_email_scraped__business_name = business_name).update(set__collection_of_email_scraped__S__postal_code = postal_code)
                                if street != ' ':
                                    MB_scraper.objects(userid = self.userid, name = self.name, collection_of_email_scraped__business_name = business_name).update(set__collection_of_email_scraped__S__Address_line1 = street)
                                if state != ' ':
                                    MB_scraper.objects(userid = self.userid, name = self.name, collection_of_email_scraped__business_name = business_name).update(set__collection_of_email_scraped__S__state = state)
                                if city != ' ':
                                    MB_scraper.objects(userid = self.userid, name = self.name, collection_of_email_scraped__business_name = business_name).update(set__collection_of_email_scraped__S__city = city)
                            else:
                                self.all_websites.append(business_name)
                                try:
                                    driver.get("http://"+websitelink.text)
                                except:
                                    print("An exception occurred")
                                    continue
                                time.sleep(10)
                                site_url = "http://"+websitelink.text
                                if site_url == "http://libertytax.com/":
                                    continue
                                websitepage = driver.page_source
                                websiteSoup = BeautifulSoup(websitepage, 'html.parser')
                                new_emails = set(re.findall(r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.[a-z0-9\.\-+_]+", websitepage, re.I))
                                only_valid = set()
                                for em in new_emails:
                                    if validate_email(em):
                                        only_valid.add(em)
                                self.no_email = False
                                self.email_counter += len(only_valid)
                                print("/n------VALID EMAIL SET------")
                                print(only_valid)
                                self.AllInternalEmails.update(only_valid)
                                self.getInternalLinks(websiteSoup, self.splitaddress(websitelink.text)[0])
                                self.AllInternalLinks.clear()

                                website_object = Website()
                                website_object.business_name = business_name
                                website_object.website_link = site_url
                                website_object.emails = list(self.AllInternalEmails)
                                website_object.keyword = self.keyword

                                if telephone != 0:
                                    website_object.telephone = telephone
                                if postal_code != 0:
                                    website_object.postal_code = postal_code
                                if street != '':
                                    website_object.Address_line1 = street
                                if state != '':
                                    website_object.state = state
                                if city != '':
                                    website_object.city = city
                                if address_line2 != '':
                                    website_object.city = address_line2
                        
                                self.AllInternalEmails.clear()
                                
                                try:
                                    MB_scraper.objects(user_id = self.userid, name = self.name).update(push__collection_of_email_scraped = website_object)
                                    MB_scraper.objects(user_id = self.userid, name = self.name).update(inc__email_counter = self.email_counter)
                                    MB_scraper.objects(user_id = self.userid, name = self.name).update(set__last_updated = datetime.datetime.now())
                                except:
                                    print("Not Unique Data")

                    
                    MB_scraper.objects(user_id = self.userid, name = self.name).update(set__status = "Scraping Completed")
                    break

            except AttributeError:
                self.flag += 1
                print(f"trial:{self.flag}")
        print('End Scraping')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('user_id',type=str,nargs='?',default='devasish',help='Enter userid')
    parser.add_argument('name',type=str,nargs='?',default='yelp_scraper',help='Enter name')
    parser.add_argument('keyword',type=str,nargs='?',default=urllib.parse.quote_plus('Realtor'),help='Enter keyword')
    parser.add_argument('city',type=str,nargs='?',default=urllib.parse.quote_plus('Manhattan Beach, CA'),help='Enter city')
    parser.add_argument('limit',type=int,nargs='?',default=24,help='Enter limit')
    args = parser.parse_args()

    userid = args.user_id
    name = args.name
    keyword = args.keyword
    city = args.city
    limit = args.limit
    print(userid,name,keyword,city,limit)

    scraper_obj = Scraper(userid,name,keyword,city,limit)
    scraper_obj.scrape(MB_scraper)