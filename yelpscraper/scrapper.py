import re
import pandas as pd
from requests import get
from bs4 import BeautifulSoup
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

class Scrapper():

  def __init__(self, userid, name, limit, keyword, loc):
    self.userid = userid
    self.name = name
    self.limit = limit
    self.keyword = keyword
    self.loc = loc
    self.internal_links = set()
    self.internal_emails = set()
    self.all_emails = set()

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-gpu")
    self.driver = webdriver.Chrome('/usr/local/bin/chromedriver', chrome_options=chrome_options)
    
  def scrap(self):
    for start in range(0, self.limit *10 + 1, 10):
      url = self.get_url(start)    
      # url = f'https://www.yelp.com/search?find_desc=Therapist&find_loc=Los+Angeles%2C+CA&ns=1&start={start}'
      print(f'page:{start}')
      self.get_emails(url)

  def get_url(self, start=0):
    # https://www.yelp.com/search?find_desc=Therapist&find_loc=San+Francisco%2C+CA&ns=1
    path = 'symbols.tsv'
    encoder = get_codes(path, 'hex')
    desc = encoder.replace_sym(self.keyword)
    loc = encoder.replace_sym(self.loc)
    url = f'https://www.yelp.com/search?find_desc={desc}&find_loc={loc}&ns=1&start={start}'
    path = 'symbols.tsv'
    return url

  def get_internal_links(self, soup, website_link):
    internal_links = []
    for links in soup.findAll("a", {"href": re.compile("^(/|.*" + website_link + ")")}):
        if links.attrs['href'] is not None and links.attrs["href"] not in internal_links:
          internal_links.append(links.attrs['href'])
    
    for link in internal_links:
      base_url = link.replace("http://", "").replace("https://", "").replace(website_link, "")
      seperator = "/"
      spliturl = base_url.split(seperator,2)
      if len(spliturl)>=2:
        base_url = spliturl[1]
        remove_param_url = spliturl[1].split("?", 1)
        if len(remove_param_url) >=1:
          base_url = remove_param_url[0]
        else:
          base_url = ""
      if base_url not in self.internal_links:
        if link != f"http://{website_link}" and link != f"https://{website_link}" and link != '/' and link != f"http://{website_link}/" and link != f"https://{website_link}/":
          self.internal_links.add(base_url)
          comp_url = f"\rhttp://{website_link}/{base_url}"
          # stdout.write(comp_url)
          try:
            websitepage = get(comp_url)
          except:
            continue
          new_emails = set(re.findall(r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.[a-z0-9\.\-+_]+", websitepage.text, re.I))
          self.internal_emails.update(new_emails)
          websitepage_soup = BeautifulSoup(websitepage.text, 'html.parser')
          self.get_internal_links(websitepage_soup,website_link)
    return (internal_links)


  def get_emails(self, url):
    self.driver.get(url)
    html = self.driver.page_source
    self.flag = 0
    self.no_email = True

    soup = BeautifulSoup(html, 'html.parser')
    page_link_modefied = soup.find_all('a',class_="lemon--a__373c0__IEZFH link__373c0__1G70M pagination-link-component__373c0__9aHoC link-color--inherit__373c0__3dzpk link-size--inherit__373c0__1VFlE")
    bussiness_list = soup.find('ul',class_="lemon--ul__373c0__1_cxs undefined list__373c0__2G8oH")
        # added loop to try loading webpage
    while self.flag < 50 and self.no_email:
      try:
        lilist = bussiness_list.findChildren(['li'])

        for li in lilist:
          # link = li.find('a',class_='lemon--a__373c0__IEZFH link__373c0__1G70M link-color--inherit__373c0__3dzpk link-size--inherit__373c0__1VFlE')
          link = li.find('a',class_='lemon--a__373c0__IEZFH link__373c0__1UGBs photo-box-link__373c0__1AMDk link-color--blue-dark__373c0__12C_y link-size--default__373c0__3m55w')
          if link == None:
              continue
          self.driver.get(f"https://www.yelp.com/{link['href']}")
          profile = self.driver.page_source
          profile_soup = BeautifulSoup(profile, 'html.parser')
          self.website_link = None
          business_website = profile_soup.find("p", string="Business website")
          if business_website != None:
            business_website_P = business_website.findNext('p')
            if business_website_P != None:
              business_website_a = business_website_P.find('a')
              if business_website_a != None:
                self.website_link = business_website_a
          if self.website_link == None:
              print(f"Link Not Found ----> https://www.yelp.com/{link['href']}")
              continue
          print(self.website_link.text)
          try:
              self.driver.get("http://" + self.website_link.text)
          except:
              print("error occurred")
              continue
          business_name = link.text
          site_url = "http://" + self.website_link.text
          websitepage = self.driver.page_source
          websiteSoup = BeautifulSoup(websitepage, 'html.parser')
          new_emails = set(re.findall(r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.[a-z0-9\.\-+_]+", websitepage, re.I))
          self.internal_emails.update(new_emails)
          self.get_internal_links(websiteSoup, self.website_link.text.replace("http://", "").replace("https://", "").split("/")[0])
          self.internal_links.clear()

          if len(self.internal_emails) == 0:
              data_dict = {"business_name": business_name,"site_url": site_url,"EmailAddress": " ","cleaned email":" ","cleaned_by":" ","cleaned_timestamp":" "}
          else:
              data_dict = {"business_name": business_name,"site_url": site_url,"EmailAddress": repr(self.internal_emails), "cleaned email":" ","cleaned_by":" ","cleaned_timestamp":" "}

          self.all_emails.add(repr(data_dict))

          self.internal_emails.clear()

            # catch block
      except AttributeError:
        self.flag += 1
        print(f"trial:{self.flag}")
      
    return self.all_emails  

  def start_database(self):
    client = pymongo.MongoClient('mongodb+srv://sumi:'+urllib.parse.quote_plus('sumi@123')+'@codemarket-staging.k16z7.mongodb.net/codemarket_akash?retryWrites=true&w=majority')
    db = client['codemarket_akash']
    collection = db['yelpscrapermailinglist']
    return collection

  def store_emails(self, all_emails = None):
    collection = self.start_database()
    if all_emails is not None:
      email_collection = repr(all_emails)
    else:
      email_collection = repr(self.all_emails)
    query = {'user_id':self.userid,'name':self.name}
    new_values = { "$set": {'created timestamp':datetime.datetime.now(),'collection of email scraped': email_collection,'status': 'Scraping Completed' } }
    collection.update_one(query,new_values)

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument('user',type=str,nargs='?',default='123',help='Enter userid')
  parser.add_argument('name',type=str,nargs='?',default='yelpscraper',help='Enter name')
  parser.add_argument('keyword',type=str,nargs='?',default=urllib.parse.quote_plus('Therapist'),help='Enter keyword')
  parser.add_argument('loc',type=str,nargs='?',default=urllib.parse.quote_plus('Los Angeles, CA'),help='Enter city')
  parser.add_argument('limit',type=int,nargs='?',default=1,help='Enter limit')
  args = parser.parse_args()

  user_id = args.user
  name = args.name
  keyword = args.keyword
  loc = args.loc
  limit = args.limit
  
  scrapper = Scrapper(user_id, name, limit, loc, keyword)
  scrapper.scrap()
