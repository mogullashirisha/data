import sys
print(sys.version)
import time
import pymongo
import urllib.parse
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities


class LinkedinScraper():
    url = "https://www.linkedin.com/login?fromSignIn=true&trk=guest_homepage-basic_nav-header-signin"

    def __init__(self, username, password, count=5):
        self.username = username
        self.password = password 
        self.count = count 

    def connect_mongo(self, db, collection):
        # Connecting with mongo Atlas to collection named jc_linkedin
        client = pymongo.MongoClient('mongodb+srv://sumi:'+urllib.parse.quote('sumi@')+'123@codemarket-staging.k16z7.mongodb.net/'+db+'?retryWrites=true&w=majority')
        # db = client.get_database(db)
        database = client[db]
        collection = database[collection]
        return collection

    def start_scraping(self, collection):
        ####### Use Below code when having chrome driver and chrome installed on env
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument('--disable-dev-shm-usage')

        # driver = webdriver.Chrome('/usr/local/bin/chromedriver',chrome_options=chrome_options)
        driver = webdriver.Chrome('C:\Python38\chromedriver',chrome_options=chrome_options)

        #######

        driver.get(self.url)
        print("linkedin opened")
        # Implicit Wait Command
        driver.implicitly_wait(5)
        # Explicit Wait command
        wait = WebDriverWait(driver, 5)
        # Getting main window handle for controlling active window
        main_handle = driver.current_window_handle


        # Enter Username
        element = wait.until(EC.visibility_of_element_located((By.XPATH, "//input[@id='username']")))
        element.clear()
        element.send_keys(self.username)

        # Enter Password
        element = wait.until(EC.visibility_of_element_located((By.XPATH, "//input[@id='password']")))
        element.clear()
        element.send_keys(self.password)

        # Click Sign In Button
        signin_btn = wait.until(EC.visibility_of_element_located((By.XPATH, "//button[@type='submit']")))
        signin_btn.click()

        # Check if otp page exists
        otps = driver.find_elements(By.XPATH, "//input[@class='form__input--text input_verification_pin']")
        if otps:
            print("OTP Page appeared")
            sys.exit("OTP Page appeared")
            # otp = otps[0]
            # otp.clear()
            # otp.send_keys(some_otp)
            # # Click Sign In Button
            # signin_btn = wait.until(EC.visibility_of_element_located((By.XPATH, "//button[@type='submit']")))
            # signin_btn.click()

        # Click My Network 
        my_network = wait.until(EC.visibility_of_element_located((By.XPATH, "//li[@id='mynetwork-nav-item']")))
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
        # for i in range (40, total_connections + 1):
        for i in range (1, self.count):
            print(i)
            connections = driver.find_elements(By.XPATH, "//li[@class='list-style-none']")
            
            # Trying to see if the index is in current list, if not use javascript to scroll page and expand connections list
            try :
                connection = driver.find_element(By.XPATH, "//li[@class='list-style-none'][{}]/div/a".format(i))
            except:
                print("page scroll exception")
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(2)
                driver.execute_script("window.scrollTo(0, 0)")
                time.sleep(2)
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(2)

            connection = driver.find_element(By.XPATH, "//li[@class='list-style-none'][{}]/div/a".format(i))
            link = connection.get_attribute('href')

            driver.switch_to.window(sub_handle)
            driver.get(link)

            # Get Name
            name = wait.until(EC.visibility_of_element_located((By.XPATH, "//div[@class='flex-1 mr5']/ul/li")))
            name = name.text
            print(name)

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
            
            print(email)

            # Saving Name, Company and Email to csv
            try:
                first_name = name.split()[0]
                last_name = name.split()[-1]
                mydict = {'first_name': first_name, 'last_name': last_name, 'company': company, 'email': email}
                collection.insert_one(mydict)
                print("Saved to Mongo")

            except:
                print("Error while writing to Mongo Atlas")

            # Switch to main window
            driver.switch_to.window(main_handle)


        # Click on My Profile Button
        myprofile = wait.until(EC.visibility_of_element_located((By.XPATH, "//li[@id='profile-nav-item']")))
        myprofile.click()

        # Click Sign Out
        signout = wait.until(EC.visibility_of_element_located((By.XPATH, "//a[text()='Sign out']")))
        signout.click()
        print("Signed out")

        # Close Browser
        driver.close()
        print("Browser closed")


# Checking if username and passwords are passed during run time 
try :
    if len(sys.argv) == 3 :
        username = sys.argv[1]
        password = sys.argv[2]
    else :
        raise NameError("Linkedin Username and Password not found")

except NameError:
    raise 

ls = LinkedinScraper(username=username, password=password)
collection = ls.connect_mongo(db='jc_linkedin', collection='profiles')
ls.start_scraping(collection=collection)
print("done")
