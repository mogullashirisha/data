#!/usr/bin/env python
# coding: utf-8

# In[1]:


from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
import urllib
import time
import re
import pandas as pd
from validate_email import validate_email
from database_code import *

# In[5]:


class Yelp:
    def __init__(self):
        print("Init Start")
        self.userid = 123
        self.name = "yelpscraper"
        self.keyword = str(urllib.parse.quote_plus('Therapist'))
        self.city = str(urllib.parse.quote_plus('Los Angeles, CA'))
        
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-gpu")
        self.driver = webdriver.Chrome('chromedriver',options=chrome_options)
        
        #self.driver = webdriver.Chrome()
        
        url = 'https://www.yelp.com/search?find_desc={}&find_loc={}&ns=1'.format(self.keyword,self.city)
        self.driver.get(url)
        #element = WebDriverWait(self.driver, 30).until(
        #    EC.presence_of_element_located((By.CLASS_NAME, "lemon--span__373c0__3997G.text__373c0__26Xrb.text-color--black-extra-light__373c0__35NWg.text-align--left__373c0__Rrl_f"))
        #)
        #time.sleep(10)
        self.driver.implicitly_wait(5)
        temp = self.driver.find_elements_by_class_name('lemon--span__373c0__3997G.text__373c0__26Xrb.text-color--black-extra-light__373c0__35NWg.text-align--left__373c0__Rrl_f')
        print(len(temp))
        temp = temp[-1].text
        print(temp)
        self.number_of_pages = int(temp.split(' ')[-1])
        print(self.number_of_pages,type(self.number_of_pages))
        print("Init End")
        
    def scrape_results(self):
        print("scrape_results Start")
        names =[]
        links = []
        driver = self.driver
        starting_points = self.number_of_pages * 10
        for i in range(0,starting_points,10):
            url = 'https://www.yelp.com/search?find_desc={}&find_loc={}&ns=1&start={}'.format(self.keyword,self.city,i)
            driver.get(url)
            driver.implicitly_wait(5)
            results = driver.find_elements_by_css_selector('a.lemon--a__373c0__IEZFH.link__373c0__1UGBs.link-color--inherit__373c0__1J-tq.link-size--inherit__373c0__3K_7i')
            print(len(results))
            page_links = results[1:]
            for page_link in page_links:
                links.append(page_link.get_attribute('href'))
                names.append(page_link.text)
            
        print("links length:",len(links))
        
        f= open("names.txt","w")
        for i in names:
            f.write(i+"\n")
        f.close()
        
        f= open("links.txt","w")
        for i in links:
            f.write(i+"\n")
        f.close()
        print("scrape_results end")
        
    def scrape_pages(self):
        print("scrape_pages Start")
        f = open('links.txt','r')
        links =[]
        for i in f:
            links.append(i.rstrip())
        f.close()
        #print(len(links))
        driver = self.driver
        websites =[]
        for i in links:
            driver.get(i)
            time.sleep(3)
            try:
                website = driver.find_element_by_css_selector("p.lemon--p__373c0__3Qnnj.text__373c0__2U54h.text-color--normal__373c0__NMBwo.text-align--left__373c0__1Uy60 > a.lemon--a__373c0__IEZFH.link__373c0__2-XHa.link-color--blue-dark__373c0__4vqlF.link-size--inherit__373c0__nQcnG").get_attribute('href')
                print(website) 
                website = website[20:]
                start = website.find("=")
                end = website.find("&website_link_type=")
                website = urllib.parse.unquote(website[start+1:end])
                print(website,len(websites))
            except Exception as e:
                print(e)
                website = ''
                print("nothing",len(websites))
            websites.append(website)
        print("len of websites:{}".format(len(websites)))
        f = open('websites.txt','w')
        for i in websites:
            f.write(i+"\n")
        f.close()
        print("scrape_pages end")
    
    def scrape_websites(self):
        print("scrape_website start")
        driver = self.driver
        names =[]
        websites = []
        emails =[]
        f = open('names.txt','r')
        for i in f:
            names.append(i.rstrip())
        f.close()
        f = open('websites.txt','r')
        for i in f:
            websites.append(i.rstrip())
        f.close()
        #print(len(websites))
        for i in websites:
            try:
                driver.get(i)
                time.sleep(3)
                all_text = driver.find_element_by_tag_name('body').text
                #print(all_text)
                web_email = list(set(re.findall(r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.[a-z0-9\.\-+_]+", all_text, re.I)))
                emails.append(web_email[0])
                print(web_email[0],len(emails))
            except:
                emails.append("")
                print("error",len(emails))
                
        
        f = open('emails.txt','w')
        for i in emails:
            f.write(i+'\n')
        f.close()
        print("scrape_website end")
        return names,websites,emails


# In[6]:


y = Yelp()
y.scrape_results()
y.scrape_pages()
names,websites,emails = y.scrape_websites()


# In[7]:


f = open('emails.txt','r')
l = 0
for i in f:
    l+=1
f.close()
print(l)


# In[9]:


df = pd.DataFrame({"Business_Name":names,
                          "Website":websites,
                          "emails":emails})
df.to_csv("Unfiltered.csv",index= False)


# In[15]:


#def email_validate(email):
#    if validate_email(email):
#        print(email,"Valid")
#    else:
#        print(email,"Not valid")

df = pd.read_csv('Unfiltered.csv')
#df.shape
'''
valid = 0
for i in range(len(df)):
    email = df.iloc[i,2]
    try:
        if validate_email(email):
            #df.iloc[i,2] = email
            valid +=1
            print(valid)
            
    except:
        df.iat[i,2] =''
'''
df.dropna(inplace=True)
df.shape
df.to_csv("filtered.csv",index = False)


# In[16]:


df.shape
db()


# In[ ]:




