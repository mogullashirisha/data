#!/usr/bin/env python
# coding: utf-8

# In[33]:


from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
import time
from bs4 import BeautifulSoup
import pandas as pd
import urllib
import requests
import re
import random
from database_insta import *


# In[37]:


class Insta:
    def __init__(self,username,password):
        self.username = username
        self.password = password
        #self.bot = webdriver.Firefox()
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")

        chrome_options.add_argument("--disable-gpu")
        self.bot = webdriver.Chrome('chromedriver',options=chrome_options)
        self.bot.get('https://www.instagram.com/akshaykumar/')
    
    def login(self):
        bot = self.bot
        bot.get('https://www.instagram.com/accounts/login/?source=auth_switcher')
        time.sleep(3)
        email = bot.find_element_by_name('username')
        password = bot.find_element_by_name('password')
        email.clear()
        password.clear()
        email.send_keys(self.username)
        password.send_keys(self.password)
        password.send_keys(Keys.RETURN)
        time.sleep(5)
        try:
            notif_dialog = bot.find_element_by_class_name('aOOlW.HoLwm').click()
        except:
            print("dialog not found")
        print("login complete")
    
    def posts(self,hashtag):
        print("def posts start")
        bot = self.bot
        bot.get('https://www.instagram.com/explore/tags/{}/'.format(hashtag))
        time.sleep(2)
        post_links = []
        for i in range(0,10):
            bot.execute_script('window.scrollTo(0,document.body.scrollHeight)')
            time.sleep(2)
            posts = bot.find_elements_by_class_name('v1Nh3.kIKUG._bz0w')
            for post in posts:
                post_links.append(post.find_element_by_css_selector('a').get_attribute('href'))
                print(len(post_links))
        print("post_links len",len(post_links))
        post_links = list(set(post_links))
        print("set post_links",len(post_links))
        f = open('post_links.txt','w')
        for i in post_links:
            f.write(i+'\n')
        f.close()
        #print("post_links len",len(post_links))
        print("def posts end")
    
    def profiles(self):
        bot = self.bot
        print("def profiles start")
        post_links = []
        f = open('post_links.txt','r')
        for i in f:
            post_links.append(i.rstrip())
        f.close()
        profiles =[]
        for i in post_links:
            bot.get(i)
            time.sleep(3)
            try:
                profile = bot.find_element_by_class_name('sqdOP.yWX7d._8A5w5.ZIAjV').get_attribute('href')
                profiles.append(profile)
                print(len(profiles))
            except Exception as e:
                print(e)
        profiles = list(set(profiles))
        print(len(profiles))
        f = open('profiles.txt','w')
        for i in profiles:
            f.write(i+'\n')
        f.close()
        print("def profiles end")
    
    def scraping_profiles(self):
        bot = self.bot
        print('def scraping_profiles start')
        profiles =[]
        f = open('profiles.txt','r')
        for i in f:
            profiles.append(i.rstrip())
        f.close()
        insta_links =[]
        names =[]
        websites =[]
        emails =[]
        for profile in profiles:
            bot.get(profile)
            time.sleep(3)
            try:
                name = bot.find_element_by_class_name('rhpdm').text
                website = bot.find_element_by_class_name('yLUwa').get_attribute('href')
                x = website.find('=')
                y = website.find('&e')
                website = urllib.parse.unquote(website[x+1:y])
                
            except Exception as e:
                print(e)
                #time.sleep(120)
                
                name =''
                website =''
            print(name,website)
            
            insta_links.append(i)
            names.append(name)
            websites.append(website)
            print(len(insta_links),len(names),len(websites))
        
        for i in websites:
            try:
                response = requests.get(i)
                soup = BeautifulSoup(response.text,'html.parser')
                all_text = soup.find('body').getText()
                #print(all_text,type(all_text))
                web_email = list(set(re.findall(r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.[a-z0-9\.\-+_]+", all_text, re.I)))
            except:
                web_email = []
            '''
                if len(web_email)>0:
                    emails.append(web_email[0])
                    print(web_email[0],len(emails))
                else:
                    emails.append('')
                    print("Didnt find",len(emails))
                time.sleep(1)
                '''
            #except:
                #emails.append('')
                #print("error",len(emails))
                
            if len(web_email) > 0:
                temp = ''
                for j in web_email:
                    try:
                        if validate_email(j):
                            emails.append(j)
                            temp = j
                            print(j,len(emails))
                            break
                    except:
                        continue
                if temp == '':
                    emails.append('')
                    print("Didnt find",len(emails))
            else:
                emails.append('')
                print("Didnt find",len(emails))

        print("ALL LENGTHS",len(insta_links),len(names),len(websites),len(emails))
        df = pd.DataFrame({'Business_Name':names,
                          'Instagram_Link':insta_links,
                          'Website':websites,
                          'Email':emails})
        df.to_csv('unfiltered.csv',index=False)
            
        print('def scraping_profiles end')
        return insta_links,names,websites,emails


# In[28]:


keyword = 'realtor'
s = Insta('_fifty_shades_of_us_','Aryman@235')
#s.login()
s.posts(keyword)
s.profiles()
insta_links,names,websites,emails = s.scraping_profiles()


# In[29]:


df = pd.read_csv('unfiltered.csv')
df.shape


# In[30]:


df.dropna(inplace=True)
df.shape


# In[31]:


df.to_csv('filtered.csv',index=False)


# In[38]:


db()


# In[ ]:




