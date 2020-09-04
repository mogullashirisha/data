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
from sym_data import get_codes
from sys import stdout
from validate_email import validate_email

class Scraper:
    def __init__(self,userid,name,keyword,city,limit):
        self.userid = userid
        self.name = name
        self.keyword = keyword
        self.city = city
        self.limit = limit
        self.counter = 0
        self.AllInternalLinks = set()
        self.AllInternalEmails = set()
        self.final_result = set()
        self.counter = 0
        self.collection = self.start_database()

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
                    print("------VALID EMAIL SET------")
                    print(only_valid)
                    self.AllInternalEmails.update(only_valid)
                    websitepage_soup = BeautifulSoup(websitepage.text, 'html.parser')
                    self.getInternalLinks(websitepage_soup,includeurl)
        return (internalLinks)

    def splitaddress(self,address):
        return (address.replace("http://", "").replace("https://", "").split("/"))

    def start_database(self):
        client = pymongo.MongoClient('mongodb+srv://sumi:'+urllib.parse.quote_plus('sumi@123')+'@codemarket-staging.k16z7.mongodb.net/codemarket_devasish?retryWrites=true&w=majority')
        db = client['codemarket_devasish']
        collection = db['yelpscrapermailinglist']
        return collection

    def get_url(self, start=0):
    # https://www.yelp.com/search?find_desc=Therapist&find_loc=San+Francisco%2C+CA&ns=
        path = 'symbols.tsv'
        encoder = get_codes(path, 'hex')
        desc = encoder.replace_sym(self.keyword)
        loc = encoder.replace_sym(self.city)
        url = f'https://www.yelp.com/search?find_desc={desc}&find_loc={loc}&ns=1&start={start}'
        path = 'symbols.tsv'
        return url

    def scrape(self):
        self.flag = 0
        self.no_email = True
        print('Begin Scraping')
        while self.no_email and self.flag < 50:
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-gpu")
            driver = webdriver.Chrome('/usr/local/bin/chromedriver',chrome_options=chrome_options)
            # driver = webdriver.Chrome('E:/Codes/chromedriver.exe',chrome_options=chrome_options)
            try:
                for start in range(0, self.limit, 10):
                    url = self.get_url(start)
                    # print(url)
                    driver.get(url)
                    html = driver.page_source
                    soup = BeautifulSoup(html, 'html.parser')
                    bussiness_list = soup.find('ul',class_="lemon--ul__373c0__1_cxs undefined list__373c0__2G8oH")
                    lilist = bussiness_list.findChildren(['li'])
                    for li in lilist:
                        status = f'Scraping website-{self.counter + 1}'
                        # link = li.find('a',class_='lemon--a__373c0__IEZFH link__373c0__1G70M link-color--inherit__373c0__3dzpk link-size--inherit__373c0__1VFlE')
                        link = li.find('a',class_='lemon--a__373c0__IEZFH link__373c0__1UGBs photo-box-link__373c0__1AMDk link-color--blue-dark__373c0__12C_y link-size--default__373c0__3m55w')
                        if link == None:
                            continue
                    
                        driver.get("https://www.yelp.com/" + link['href'])
                        time.sleep(7)
                        profile = driver.page_source
                        profile_soup = BeautifulSoup(profile, 'html.parser')
                        websitelink = None
                        business_name = profile_soup.find('h1',class_ = "lemon--h1__373c0__2ZHSL heading--h1__373c0___56D3 undefined heading--inline__373c0__1jeAh").text
                        print(business_name)
                        if profile_soup.find("p", string="Business website") != None:
                            if profile_soup.find("p", string="Business website").findNext('p') != None:
                                if profile_soup.find("p", string="Business website").findNext('p').find('a') != None:
                                    websitelink = profile_soup.find("p", string="Business website").findNext('p').find('a')
                        if websitelink == None:
                            print("Link Not Found")
                            print("https://www.yelp.com/" + link['href'])
                            continue
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
                        print("------VALID EMAIL SET------")
                        print(only_valid)
                        self.AllInternalEmails.update(only_valid)
                        self.getInternalLinks(websiteSoup, self.splitaddress(websitelink.text)[0])
                        self.AllInternalLinks.clear()

                        if len(self.AllInternalEmails) == 0:
                            data_dict = {"business_name": business_name,"site_url": site_url,"Emails": " "}#,"cleaned email":" ","cleaned_by":" ","cleaned_timestamp":" "}
                        else:
                            data_dict = {"business_name": business_name,"site_url": site_url,"Emails": self.AllInternalEmails }#,"cleaned email":" ","cleaned_by":" ","cleaned_timestamp":" "}
                 
                        self.final_result.add(repr(data_dict))
                
                        self.AllInternalEmails.clear()
                        self.counter += 1

                        if self.counter == self.limit:
                            status = 'Scraping Completed'
                            self.store_emails(status)
                            break
                        
                        self.store_emails(status)
                break

            except AttributeError:
                self.flag += 1
                print(f"trial:{self.flag}")
        print('End Scraping')

    def store_emails(self, status):
        print('Updating Database')
        email_collection = repr(self.final_result)
        query = {'user_id':self.userid,'name':self.name}
        newvalues = { "$set": {'limit':self.limit, 'created timestamp':datetime.datetime.now(),'collection of email scraped': email_collection,'status': status } }
        self.collection.update_one(query,newvalues)
        print('Database Updated')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('user_id',type=str,nargs='?',default='devasish',help='Enter userid')
    parser.add_argument('name',type=str,nargs='?',default='yelpscraper',help='Enter name')
    parser.add_argument('keyword',type=str,nargs='?',default=urllib.parse.quote_plus('Therapist'),help='Enter keyword')
    parser.add_argument('city',type=str,nargs='?',default=urllib.parse.quote_plus('Los Angeles, CA'),help='Enter city')
    parser.add_argument('limit',type=int,nargs='?',default=15,help='Enter limit')
    args = parser.parse_args()

    userid = args.user_id
    name = args.name
    keyword = args.keyword
    city = args.city
    limit = args.limit
    print(userid,name,keyword,city,limit)

    scraper_obj = Scraper(userid,name,keyword,city,limit)
    scraper_obj.scrape()