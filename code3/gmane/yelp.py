import sqlite3
import time
import ssl
import json
import urllib.request, urllib.parse, urllib.error
from urllib.parse import urljoin
from urllib.parse import urlparse

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

conn = sqlite3.connect('yelp.sqlite')
cur = conn.cursor()

# File is hosted locally just to emulate HTTP connection
baseurl = "http://127.0.0.1:8000/dev/py4e/yelp_dataset/"
url = baseurl + 'yelp_academic_dataset_review.json'
url_b = baseurl + 'yelp_academic_dataset_business.json'

cur.execute('DROP TABLE IF EXISTS Reviews')
cur.execute('DROP TABLE IF EXISTS Businesses')
cur.execute('DROP TABLE IF EXISTS Categories')
cur.execute('DROP TABLE IF EXISTS Bis_Cats')

cur.execute('''CREATE TABLE IF NOT EXISTS Reviews
    (id TEXT NOT NULL PRIMARY KEY UNIQUE, user_id TEXT, business_id TEXT,
     stars REAL, review TEXT, date TEXT)''')

cur.execute('''CREATE TABLE IF NOT EXISTS Businesses
    (id TEXT NOT NULL PRIMARY KEY UNIQUE, name TEXT, city TEXT,
     state TEXT, is_open INTEGER)''')

cur.execute('''CREATE TABLE IF NOT EXISTS Categories
    (id INTEGER NOT NULL PRIMARY KEY UNIQUE, name TEXT UNIQUE)''')

cur.execute('''CREATE TABLE IF NOT EXISTS Bis_Cats
    (business_id TEXT, category_id INTEGER, PRIMARY KEY(business_id, category_id))''')

document = urllib.request.urlopen(url, None, 30, context=ctx)
count = 0

# Copy reviews to a database
for line in document:
    count += 1
    print(count, 'stage 0') # Track progress
    data = json.loads(line.decode())
    cur.execute('''INSERT OR IGNORE INTO Reviews (id, user_id, business_id, stars, review, date)
    VALUES ( ?, ?, ?, ?, ?, ? )''', ( data['review_id'], data['user_id'], data['business_id'], data['stars'], data['text'], data['date'] ))
    if count % 100000 == 0:
        conn.commit()
conn.commit()

document = urllib.request.urlopen(url_b, None, 30, context=ctx)
count = 0
cat_count = 0
categories = {}

# Copy businesses to a database and populate categories dictionary
for line in document:
    count += 1
    print(count, 'stage 1') # Track progress
    data = json.loads(line.decode())
    cur.execute('''INSERT OR IGNORE INTO Businesses (id, name, city, state, is_open)
    VALUES ( ?, ?, ?, ?, ? )''', ( data['business_id'], data['name'], data['city'], data['state'], data['is_open'] ))
    if data['categories']:
        business_cats = data['categories'].split(',')
        for cat in business_cats:
            if cat.strip() not in categories:
                cat_count += 1
                categories[cat.strip()] = cat_count
conn.commit()

# Copy categories to a database
for (key, value) in categories.items():
    cur.execute('''INSERT OR IGNORE INTO Categories (id, name)
    VALUES ( ?, ? )''', ( value, key ))
conn.commit()

count = 0

# Map categories to businesses in a many-to-many database
for line in document:
    count += 1
    print(count, 'stage 2') # Track progress 
    data = json.loads(line.decode())
    if data['categories']:
        business_cats = data['categories'].split(',')
        for cat in business_cats:
            # Use dictionary instead of SELECT, it is faster 
            #cur.execute('SELECT id FROM Categories WHERE name=? LIMIT 1', ( cat, ))
            #row = cur.fetchone()
            #cat_id = row[0]
            cat_id = categories[cat.strip()]
            cur.execute('''INSERT OR IGNORE INTO Bis_Cats (business_id, category_id)
            VALUES ( ?, ? )''', ( data['business_id'], cat_id ))
conn.commit()

cur.close()

# Data examples: https://www.yelp.com/dataset/documentation/main

# SQL select example
# SELECT Businesses.name, Categories.name
# FROM Businesses
# JOIN Bis_Cats JOIN Categories
# ON Bis_Cats.business_id = Businesses.id AND
# Bis_Cats.category_id = Categories.id
# LIMIT 10
