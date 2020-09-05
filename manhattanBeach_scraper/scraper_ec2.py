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
from sys import stdout
from validate_email import validate_email

class Scraper:
    def __init__(self,userid,name,limit):
        self.userid = userid
        self.name = name
        self.limit = limit
        self.counter = 0
        self.AllInternalLinks = set()
        self.AllInternalEmails = set()
        self.AllEmails = {}
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
                    if len(only_valid) > 0:
                        print("------VALID EMAIL SET------")
                        print(only_valid)
                    self.AllInternalEmails.update(only_valid)
                    websitepage_soup = BeautifulSoup(websitepage.text, 'html.parser')
                    self.getInternalLinks(websitepage_soup, includeurl)
        return (internalLinks)

    def splitaddress(self,address):
        return (address.replace("http://", "").replace("https://", "").split("/"))

    def start_database(self):
        client = pymongo.MongoClient('mongodb+srv://sumi:'+urllib.parse.quote_plus('sumi@123')+'@codemarket-staging.k16z7.mongodb.net/codemarket_devasish?retryWrites=true&w=majority')
        db = client['codemarket_devasish']
        collection = db['manhattanBeach_scraper']
        return collection

    def scrape(self):
        self.flag = 0
        self.no_email = True
        print('Begin Scraping')
        # while self.no_email and self.flag < 50:
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-gpu")
        self.driver = webdriver.Chrome('/usr/local/bin/chromedriver',chrome_options=chrome_options)
        # self.driver = webdriver.Chrome('E:/Codes/chromedriver.exe',chrome_options=chrome_options)
        url = "https://business.manhattanbeachchamber.com/members"
        # print(url)
        self.driver.get(url)
        html = self.driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        bussiness_list = soup.find("div", id="gz-ql")
        lilist = bussiness_list.find('ul').findChildren(['li'])
        for li in lilist:
            cate = li.findChildren(['a'])
            self.scrape_category(cate[0])

    def scrape_category(self, cate):
        category = cate.text
        category_url = cate['href']
        self.driver.get(category_url)
        html = self.driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        business_list = soup.findAll('div',class_="card-header")
        for li in business_list:
            business = li.find('a')
            business_name = business['alt']
            print(business_name)
            profilelink = business['href']
            status = f'Scraping website-{self.counter + 1}'

            self.driver.get(profilelink)
            time.sleep(7)
            profile = self.driver.page_source
            profile_soup = BeautifulSoup(profile, 'html.parser')
            website = profile_soup.find('a', itemprop = "url")
            websitelink = website['href']
            
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
            if len(only_valid) > 0:
                print("------VALID EMAIL SET------")
                print(only_valid)
            self.AllInternalEmails.update(only_valid)
            self.getInternalLinks(websiteSoup, self.splitaddress(websitelink)[0])
            self.AllInternalLinks.clear()

            if len(self.AllInternalEmails) == 0:
                data_dict = {"business_name": business_name,"site_url": websitelink, "Category": category, "Emails": " "}
            else:
                data_dict = {"business_name": business_name,"site_url": websitelink, "Category": category, "Emails": self.AllInternalEmails }
        
            self.final_result.add(repr(data_dict))
    
            self.AllInternalEmails.clear()

            self.counter += 1

            if self.counter == self.limit:
                status = 'Scraping Completed'
                self.store_emails(status)
                break
            
            self.store_emails(status)

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
    parser.add_argument('name',type=str,nargs='?',default='manhattanbeach_scraper',help='Enter name')
    parser.add_argument('limit',type=int,nargs='?',default=2,help='Enter limit')
    args = parser.parse_args()

    userid = args.user_id
    name = args.name
    limit = args.limit
    print(userid,name,limit)

    scraper_obj = Scraper(userid,name,limit)
    scraper_obj.scrape() 