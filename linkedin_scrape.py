import sys
print(sys.version)
import time
import pymongo
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import urllib.parse
from mongoengine import *
import pandas as pd
from mongoengine.context_managers import switch_collection
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import datetime

class linkedin_connection(EmbeddedDocument):
    linkedin = StringField()
    first_name = StringField()
    last_name = StringField()
    company = StringField()
    email = StringField()
    website = StringField()
    twitter = StringField()


class linkedin_scraper(Document):
    userid = StringField()
    connection_details = EmbeddedDocumentListField(linkedin_connection)
    created_timestamp = DateTimeField()
    last_updated = DateTimeField()

def get_scraped_data():
    client = pymongo.MongoClient('mongodb+srv://sumi:'+urllib.parse.quote_plus('sumi@123')+'@codemarket-staging.k16z7.mongodb.net/codemarket_devasish?retryWrites=true&w=majority')
    query={'userid': username}
    db = client["codemarket_devasish"]
    collection = db["LinkedIn"]
    document = collection.find_one(query)
    data = document["connection_details"]
    dataframe = pd.DataFrame(data)
    col = dataframe.linkedin.to_list()
    return col

# Checking if username and passwords are passed during run time 
try :
    if len(sys.argv) == 3 :
        username = sys.argv[1]
        password = sys.argv[2]
    else :
        raise NameError("Linkedin Username and Password not found")

except NameError:
    raise

#The mail addresses and password
sender_address = 'wrath.devasishmahato@gmail.com'
sender_pass = '123codemarket$$'
receiver_address = username
#Setup the MIME
message = MIMEMultipart()
message['From'] = sender_address
message['To'] = receiver_address
message['Subject'] = 'Scraping notification from CodeMarket'   #The subject line

url = "https://www.linkedin.com/login?fromSignIn=true&trk=guest_homepage-basic_nav-header-signin"

try:
    all_connections = get_scraped_data()
except:
    all_connections = []

# Connecting with mongo Atlas to collection named jc_linkedin
connect(db = 'codemarket_devasish', host = 'mongodb+srv://sumi:'+urllib.parse.quote_plus('sumi@123')+'@codemarket-staging.k16z7.mongodb.net/codemarket_devasish?retryWrites=true&w=majority')
with switch_collection(linkedin_scraper, 'LinkedIn') as linkedin_scraper:
    if len(all_connections) == 0:
        ls = linkedin_scraper()
        ls.userid = username
        ls.created_timestamp = datetime.datetime.now()
        ls.save()

    ####### Use Below code when having chrome driver and chrome installed on env
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-gpu")
    # chrome_options.add_argument('--disable-dev-shm-usage')
    # driver = webdriver.Chrome('/usr/local/bin/chromedriver',chrome_options=chrome_options)
    #driver = webdriver.Chrome('E:/Codes/chromedriver.exe', chrome_options=chrome_options)
    driver = webdriver.Chrome(r'C:\Users\Siddharth\CPT\codes\chromedriver.exe')
	
    driver.get(url)
    print("linkedin opened")
    # Implicit Wait Command
    driver.implicitly_wait(10)
    # Explicit Wait command
    wait = WebDriverWait(driver, 50)
    # Getting main window handle for controlling active window
    main_handle = driver.current_window_handle

    # Enter Username
    element = wait.until(EC.visibility_of_element_located((By.XPATH, "//input[@id='username']")))
    element.clear()
    element.send_keys(username)

    # Enter Password
    element = wait.until(EC.visibility_of_element_located((By.XPATH, "//input[@id='password']")))
    element.clear()
    element.send_keys(password)

    # Click Sign In Button
    signin_btn = wait.until(EC.visibility_of_element_located((By.XPATH, "//button[@type='submit']")))
    signin_btn.click()

    # Check if otp page exists
    otps = driver.find_elements(By.XPATH, "//input[@class='form__input--text input_verification_pin']")
    if otps:
        print("OTP Page appeared")
        # otp = otps[0]
        # otp.clear()
        # element.send_keys(password)
        # # Click Sign In Button
        # signin_btn = wait.until(EC.visibility_of_element_located((By.XPATH, "//button[@type='submit']")))
        # signin_btn.click()
        
        # if otp page opened, send an email to user
        mail_content_otp = '''Hello,
        Greetings from CodeMarket,

        This is to inform you that we bumped on an otp page while trying to login your linkedin account for your scraping request. 
        '''

        #The body and the attachments for the mail
        # message.attach(MIMEText(mail_content_otp, 'plain'))
        # #Create SMTP session for sending the mail
        # session = smtplib.SMTP('smtp.gmail.com', 587) #use gmail with port
        # session.starttls() #enable security
        # session.login(sender_address, sender_pass) #login with mail_id and password
        # text = message.as_string()
        # session.sendmail(sender_address, receiver_address, text)
        # session.quit()
        # print('OTP screen received while trying to login, email sent to user')


    # if otp page not opened and login successful, send an email to user
    mail_content = '''Hello,
    Greetings from CodeMarket,

    This is to inform you that we have started scraping your linkedin as per your request.

    We will update you once we have completed the scraping process. 
    '''

    #The body and the attachments for the mail
    # message.attach(MIMEText(mail_content, 'plain'))
    # #Create SMTP session for sending the mail
    # session = smtplib.SMTP('smtp.gmail.com', 587) #use gmail with port
    # session.starttls() #enable security
    # session.login(sender_address, sender_pass) #login with mail_id and password
    # text = message.as_string()
    # session.sendmail(sender_address, receiver_address, text)
    # session.quit()
    # print('scraping started, email sent to user')

    # Click My Network
    my_network = wait.until(EC.visibility_of_element_located((By.XPATH, "//a[@data-control-name='nav.mynetwork']")))
    # driver.get('https://www.linkedin.com/mynetwork/')
    print("On my network page")
    my_network.click()

    # Get Total Connections and convert it to integer
    connect_val = wait.until(EC.visibility_of_element_located((By.XPATH, "//a[@data-control-name='connections']/div/div[2]")))
    total_connections = connect_val.text
    total_connections = int(total_connections.replace(',', ''))
    print("Total Connections : ", total_connections)

    # Click Connections 
    connections = wait.until(EC.visibility_of_element_located((By.XPATH, "//a[@data-control-name='connections'][1]")))
    connections.click()

    # Open new window, save new window handle and jump back to main window
    driver.execute_script("window.open('')")
    driver.switch_to.window(driver.window_handles[-1])
    # Getting second window handle for controlling active window
    sub_handle = driver.current_window_handle
    driver.switch_to.window(main_handle)

    # Get Connections list and loop over each connection and get its name, company name, email and save to csv file
    for i in range (1, total_connections + 1):
    # for i in range (1, 5):
        print(i)
        connections = driver.find_elements(By.XPATH, "//a[@class='mn-connection-card artdeco-list ember-view']")
        
        # Trying to see if the index is in current list, if not use javascript to scroll page and expand connections list
        try :
            connection = driver.find_element(By.XPATH, "//li[@class='mn-connection-card artdeco-list ember-view'][{}]/div/a".format(i))
        except:
            print("page scroll exception")
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(2)
            driver.execute_script("window.scrollTo(0, 0)")
            time.sleep(2)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(2)

        connection = driver.find_element(By.XPATH, "//li[@class='mn-connection-card artdeco-list ember-view'][{}]/div/a".format(i))
        link = connection.get_attribute('href')

        driver.switch_to.window(sub_handle)
        driver.get(link)

        # Get Name
        try:
            name = wait.until(EC.visibility_of_element_located((By.XPATH, "//div[@class='flex-1 mr5']/ul/li")))
            name = name.text
            print(name)
        except:
            continue

        # Get Company Name
        try:
            company = wait.until(EC.visibility_of_element_located((By.XPATH, "//ul[@class='pv-top-card--experience-list']/li/a/span[1]")))
            company = company.text
        except:
            company = None    

        print(company)

        # Click Contact Info 
        connections = wait.until(EC.visibility_of_element_located((By.XPATH, "//a[@data-control-name='contact_see_more']")))
        connections.click()

        # Get email if available 
        popup = wait.until(EC.visibility_of_element_located((By.XPATH, "//h2[@class='pv-profile-section__card-heading mb4']")))
        try:
            email = wait.until(EC.visibility_of_element_located((By.XPATH, "//section[@class='pv-contact-info__contact-type ci-email']/div/a")))
            email = email.text
        except:
            email = None
        try:
            linkedin = wait.until(EC.visibility_of_element_located((By.XPATH, "//section[@class='pv-contact-info__contact-type ci-vanity-url']/div/a")))
            linkedin = linkedin.text
        except:
            continue
        try:
            twitter = wait.until(EC.visibility_of_element_located((By.XPATH, "//section[@class='pv-contact-info__contact-type ci-twitter']/ul/li/a")))
            twitter = twitter.text
        except:
            twitter = None
        try:
            website = wait.until(EC.visibility_of_element_located((By.XPATH, "//section[@class='pv-contact-info__contact-type ci-website']/ul/li/div/a")))
            website = website.text
        except:
            website = None
        
        print(email)

        # Saving Name, Company and Email to csv
        first_name = name.split()[0]
        last_name = name.split()[-1]
        lc = linkedin_connection()
        lc.first_name = first_name
        lc.last_name = last_name
        lc.company = company
        lc.email = email
        lc.linkedin = linkedin
        lc.website = website
        lc.twitter = twitter
        try:
            if linkedin not in all_connections:
                linkedin_scraper.objects(userid = username).update(push__connection_details = lc)
            else:
                if twitter != None:
                    linkedin_scraper.objects(userid = username, connection_details__linkedin = linkedin).update(set__connection_details__S__twitter =  twitter)
                if website != None:
                    linkedin_scraper.objects(userid = username, connection_details__linkedin = linkedin).update(set__connection_details__S__website = website)
                if email != None:
                    linkedin_scraper.objects(userid = username, connection_details__linkedin = linkedin).update(set__connection_details__S__email = email)
                if company != None:
                    linkedin_scraper.objects(userid = username, connection_details__linkedin = linkedin).update(set__connection_details__S__company = company)
                linkedin_scraper.objects(userid = username, connection_details__linkedin = linkedin).update(set__connection_details__S__first_name = first_name)
                linkedin_scraper.objects(userid = username, connection_details__linkedin = linkedin).update(set__connection_details__S__last_name = last_name)
            linkedin_scraper.objects(userid = username).update(set__last_updated = datetime.datetime.now())
            print("Saved to Mongo")

        except:
            print("Error while writing to Mongo Atlas")

        # Switch to main window
        driver.switch_to.window(main_handle)


# Click on My Profile Button
# myprofile = wait.until(EC.visibility_of_element_located((By.XPATH, "//li[@id='profile-nav-item']")))
myprofile = wait.until(EC.visibility_of_element_located((By.XPATH, "//button[@id='ember36']")))
myprofile.click()

# Click Sign Out
# signout = wait.until(EC.visibility_of_element_located((By.XPATH, "//a[text()='Sign out']")))
# signout.click()
# print("Signed out")

# if otp page not opened and login successful, send an email to user
mail_content_done = '''Hello,
Greetings from CodeMarket,

This is to inform you that we have completed scraping your linkedin as per your request.

Many thanks for working with us. 
'''

#The body and the attachments for the mail
message.attach(MIMEText(mail_content_done, 'plain'))
#Create SMTP session for sending the mail
session = smtplib.SMTP('smtp.gmail.com', 587) #use gmail with port
session.starttls() #enable security
session.login(sender_address, sender_pass) #login with mail_id and password
text = message.as_string()
session.sendmail(sender_address, receiver_address, text)
session.quit()
print('scraping completed, email sent to user')

# Close Browser
driver.close()
print("Browser closed")
