import requests_toolbelt
import requests
from bs4 import BeautifulSoup
import re
import numpy as np
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import time
from requests_toolbelt.threaded import pool
import pandas as pd
from pymongo import MongoClient
import datetime
import pymongo
import yfinance
import threading

client = MongoClient('10.0.0.142')
db = client['whales']
collection = db['alerts']

options = webdriver.ChromeOptions()
options.add_argument("start-maximized")
options.add_argument("--headless")
options.add_argument("--log-level=3")
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option('useAutomationExtension', False)
url = 'https://twitter.com/unusual_whales'

def get_tweets():
    print("Getting tweets")
    try:
        driver = webdriver.Chrome(executable_path='C:\\Users\\nbrei\\Documents\\GitHub\\OptimalEve\\chromedriver.exe', options=options)
        driver.get(url)

        x = WebDriverWait(driver, 200).until(EC.visibility_of_element_located((By.CSS_SELECTOR, 'div[data-testid="tweet"]')))
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(5)
        tweets = driver.find_elements_by_css_selector('div[data-testid="tweet"]')
    except Exception as e:
        print("Getting tweets error", e)

    parse_tweets(tweets)

def parse_tweets(tweets):
    print("Parsing tweets")
    for tweet in tweets:
        if "Find out more" not in tweet.text:
            continue
        alert = {}
        for row in tweet.text.split('\n'):
            items = row.split(': ')
            if len(items)<2 and not (' 2021' in str(items) or ' 2022' in str(items)):
                continue
            if ' 2021' in str(items) or '2022' in str(items):
                items = items[0].split(' ')
                alert['Ticker'] = items[0]
                alert['Expiration'] = items[1]
                alert['Type'] = items[2]
                alert['Strike'] = items[3]
                alert['Alert Datetime'] = datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
                alert['Alert Datetime Epoch'] = int(time.time())
                alert['Bid Prices'] = []
                alert['Ask Prices'] = []
            else:
                if 'Bid' in str(items[0]):
                    alert['Starting Bid'] = items[1].split(' - ')[0].replace('$','')
                    alert['Starting Ask'] = items[1].split(' - ')[1].replace('$', '')
                else:
                    alert[items[0]] = items[1]

        try:
            collection.insert_one(alert)
            print("New alert inserted", alert['Ticker'])
        except:
            #print('got duplicate key error', alert['Ticker'])
            pass


def update_prices():
    print("Updating prices")
    alerts = collection.find()
    for alert in alerts:
        alert_datetime = datetime.datetime.strptime(alert['Alert Datetime'], "%m/%d/%Y, %H:%M:%S").date()
        current_datetime = datetime.datetime.now().date()
        if np.busday_count(alert_datetime, current_datetime)>5:
            continue

        try:
            stock = yfinance.Ticker(alert['Ticker'].replace('$',''))

            expiration = datetime.datetime.strptime(alert['Expiration'], '%Y-%m-%d')
            expiration = expiration - datetime.timedelta(days=1)

            calls_and_puts = stock.option_chain(expiration.strftime('%Y-%m-%d'))
        except:
            time.sleep(1)
            continue
        if alert['Type'] == 'C':
            options = calls_and_puts[0]
        else:
            options = calls_and_puts[1]
        strike = alert['Strike']
        options = options[options['strike']==float(strike.replace('$',''))]


        bid_price = options['bid'].values[0]
        ask_price = options['ask'].values[0]

        alert['Bid Prices'] = alert['Bid Prices'] + [bid_price]
        alert['Ask Prices'] = alert['Ask Prices'] + [ask_price]

        alert['Bid Price Mean'] = round(sum(alert['Bid Prices'])/len(alert['Bid Prices']),2)
        alert['Ask Price Mean'] = round(sum(alert['Ask Prices'])/len(alert['Ask Prices']),2)

        alert['Bid Price Max'] = max(alert['Bid Prices'])
        alert['Ask Price Max'] = max(alert['Ask Prices'])

        alert['Bid Price Min'] = min(alert['Bid Prices'])
        alert['Ask Price Min'] = min(alert['Ask Prices'])

        try:
            collection.replace_one({'_id': alert['_id']}, alert)

            print('Prices updated', alert['Ticker'],
                  round(float(alert['Bid Price Mean'])-float(alert['Starting Bid']),2),
                  round(float(alert['Ask Price Mean'])-float(alert['Starting Ask']),2)
                 )
        except:
            pass



def get_tweets_thread():
    db['alerts'].create_index(
        [("Ticker", pymongo.DESCENDING), ("Strike", pymongo.DESCENDING), ("Expiration", pymongo.DESCENDING), ("Type", pymongo.DESCENDING)],
    unique=True
    )
    while True:
        try:
            get_tweets()
        except Exception as e:
            print("TWEETS BIG ERROR", e)
        next_update = time.time()+120

        while time.time()<next_update:
            time.sleep(1) 

        if datetime.datetime.now().hour>=15:
            print('exiting')
            break

def update_prices_thread():
    time.sleep(30)
    while True:
        try:
            update_prices()
        except Exception as e:
            print("PRICES BIG ERROR", e)
        next_update = time.time()+600

        while time.time()<next_update:
            time.sleep(1)
        if datetime.datetime.now().hour>=15:
            print('exiting')
            break

if __name__ == "__main__":
    tweets_thread = threading.Thread(target=get_tweets_thread)
    tweets_thread.start()
    prices_thread = threading.Thread(target=update_prices_thread)
    prices_thread.start()

    prices_thread.join()
