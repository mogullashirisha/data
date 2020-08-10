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

class Scraper:
    
    def __init__(self,name,keyword,city,limit):
        self.name = name
        self.keyword = keyword
        self.city = city
        self.limit = limit
        self.counter = 2
        self.AllInternalLinks = set()
        self.AllInternalEmails = set()
        self.final_result = set()
        

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
                     #print("http://"+includeurl+"/" + truncURL)
                     try:
                         websitepage = get("http://"+includeurl+"/" + truncURL)
                     except:
                         continue
                     new_emails = set(re.findall(r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.[a-z0-9\.\-+_]+", websitepage.text, re.I))
                     self.AllInternalEmails.update(new_emails)
                     websitepage_soup = BeautifulSoup(websitepage.text, 'html.parser')
                     self.getInternalLinks(websitepage_soup,includeurl)
        return (internalLinks)



    def splitaddress(self,address):
        return (address.replace("http://", "").replace("https://", "").split("/"))

    def start_database(self):
        client = pymongo.MongoClient('mongodb+srv://sumi:'+urllib.parse.quote_plus('sumi@123')+'@codemarket-staging.k16z7.mongodb.net/codemarket_akash?retryWrites=true&w=majority')
        db = client['codemarket_akash']
        collection = db['yelpscrapermailinglist']
        return collection

    def scrape(self):
        url = 'https://www.yelp.com/search?find_desc='+self.keyword+'&find_loc='+self.city+'&ns=1'
        print(url)

        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-gpu")
        driver = webdriver.Chrome('/usr/local/bin/chromedriver',chrome_options=chrome_options)

        print('Get URL')
        driver.get(url)
        time.sleep(7)
        html = driver.page_source

        html_soup = BeautifulSoup(html, 'html.parser')

        updatedAllLinkPage = html_soup.find_all('a',class_="lemon--a__373c0__IEZFH link__373c0__1G70M pagination-link-component__373c0__9aHoC link-color--inherit__373c0__3dzpk link-size--inherit__373c0__1VFlE")
        bussinessList = html_soup.find('ul',class_="lemon--ul__373c0__1_cxs undefined list__373c0__2G8oH")
        lilist = bussinessList.findChildren(['li'])


        print('Begin Scraping')
        
        for x in range(1, limit): 
            alllinks = updatedAllLinkPage
            for a in alllinks:
                if a.text == str(self.counter) and int(a.text) <= self.limit:
                    #print('a.text: ',a.text,'x: ',x,'counter: ',self.counter)
                    self.counter = self.counter + 1
                    driver.get("https://www.yelp.com/" + a['href'])
                    print("https://www.yelp.com/" + a['href'])
                    time.sleep(7)
                    page = driver.page_source
                    page_soup = BeautifulSoup(page, 'html.parser')
                    bussinessList1 = page_soup.find('ul', class_="lemon--ul__373c0__1_cxs undefined list__373c0__2G8oH")
                    
                    lilist1 = bussinessList1.findChildren(['li'])
                    updatedAllLinkPage = page_soup.find_all('a',class_="lemon--a__373c0__IEZFH link__373c0__1G70M pagination-link-component__373c0__9aHoC link-color--inherit__373c0__3dzpk link-size--inherit__373c0__1VFlE")

                    for li in lilist1:
                        link = li.find('a',class_='lemon--a__373c0__IEZFH link__373c0__1G70M link-color--inherit__373c0__3dzpk link-size--inherit__373c0__1VFlE')
                        if link == None:
                            continue
                        #print("https://www.yelp.com/" + link['href'])
                        #print(link.text)
                        driver.get("https://www.yelp.com/" + link['href'])
                        time.sleep(7)
                        profile = driver.page_source
                        profile_soup = BeautifulSoup(profile, 'html.parser')
                        websitelink = None
                        if profile_soup.find("p", string="Business website") != None:
                            if profile_soup.find("p", string="Business website").findNext('p') != None:
                                if profile_soup.find("p", string="Business website").findNext('p').find('a') != None:
                                    websitelink = profile_soup.find("p", string="Business website").findNext('p').find('a')
                        if websitelink == None:
                            print("Link Not Found")
                            print("https://www.yelp.com/" + link['href'])
                            continue
                        #print(websitelink.text)
                        try:
                            driver.get("http://"+websitelink.text)
                        except:
                            print("An exception occurred")
                            continue
                        time.sleep(10)
                        business_name = link.text
                        site_url = "http://"+websitelink.text
                        if site_url == "http://libertytax.com/":
                            continue
                        websitepage = driver.page_source
                        websiteSoup = BeautifulSoup(websitepage, 'html.parser')
                        new_emails = set(re.findall(r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.[a-z0-9\.\-+_]+", websitepage, re.I))
                        self.AllInternalEmails.update(new_emails)
                        self.getInternalLinks(websiteSoup, self.splitaddress(websitelink.text)[0])
                        self.AllInternalLinks.clear()
                        #print(self.AllInternalEmails)

                        if len(self.AllInternalEmails) == 0:
                            data_dict = {"business_name": business_name,"site_url": site_url,"Emails": " ","cleaned email":" ","cleaned_by":" ","cleaned_timestamp":" "}
                        else:
                            data_dict = {"business_name": business_name,"site_url": site_url,"Emails": self.AllInternalEmails,"cleaned email":" ","cleaned_by":" ","cleaned_timestamp":" "}

                       
                        self.final_result.add(repr(data_dict))
             
                        self.AllInternalEmails.clear()
        print('End Scraping')
        collection = self.start_database()
        email_collection = repr(self.final_result)
        query = {'name':self.name}
        newvalues = { "$set": {'created timestamp':datetime.datetime.now(),'collection of email scraped': email_collection,'status': 'Scraping Completed' } }
        collection.update_one(query,newvalues)

        
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('name',type=str,nargs='?',default='yelpscraper',help='Enter name')
    parser.add_argument('keyword',type=str,nargs='?',default=urllib.parse.quote_plus('Therapist'),help='Enter keyword')
    parser.add_argument('city',type=str,nargs='?',default=urllib.parse.quote_plus('Los Angeles, CA'),help='Enter city')
    parser.add_argument('limit',type=int,nargs='?',default=1,help='Enter limit')
    args = parser.parse_args()

    name = args.name
    keyword = args.keyword
    city = args.city
    limit = args.limit+1
    print(name,keyword,city,limit)

    scraper_obj = Scraper(name,keyword,city,limit)
    scraper_obj.scrape()







                
                
        
