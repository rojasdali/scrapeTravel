__author__ = 'Dali'

# Import packages
import requests
import re
from bs4 import BeautifulSoup
import sqlite3
import time

# make a database
conn = sqlite3.connect('traveldeals.db')
c = conn.cursor()

# make the table for fly4free scraping, include constraint on the links so they will never be duplicates
c.execute ('''CREATE TABLE IF NOT EXISTS offers
                 (deal_id INTEGER PRIMARY KEY,
                 links TEXT NOT NULL,
                 title TEXT,
                 description TEXT,
                 cost INTEGER,
                UNIQUE (links))''')

c.execute ('''CREATE TABLE IF NOT EXISTS trip
                 (destination_id INTEGER PRIMARY KEY,
                 deal_id INTEGER,
                 departure_city TEXT,
                 destination_city TEXT,
                 title TEXT,
                 description TEXT,
                 price_low INTEGER,
                 price_high INTEGER,
                 dates DATE,
                FOREIGN KEY(deal_id) REFERENCES offers(deal_id))''')

c.execute ('''CREATE TABLE IF NOT EXISTS offers_images
                 (image_id INTEGER PRIMARY KEY,
                 deal_id INTEGER,
                 title text,
                 description text,
                 FOREIGN KEY(deal_id) REFERENCES offers(deal_id))''')

# Specify url that we want to initially parse: url
url = 'http://www.fly4free.com/flights/flight-deals/usa/'


# Package the request, send the request and catch the response: r
r = requests.get(url)

# Extracts the response as html: html_doc
html_doc = r.text

# Create a BeautifulSoup object from the HTML: soup
soup = BeautifulSoup(html_doc, "html.parser")

# declare a list of usa_links (grabbed from scraping) and dblinks (grabbed from created db)
usa_links = []
dblinks = []

# list of titles that get scraped
titles = []

# list of months that we use to pull data only containing the 12 months for 'dates_list'
months = ['January','February','March','April', 'May', 'June', 'July','August','September', 'October', 'November', 'December']

# list of dates that get scraped
dates_list = []

# Print and append the URLs from fly4free to the shell
for h3 in soup.find_all('h3'):
     for link in h3.find_all('a'):
         str_link = str(link.get('href'))
         if "usa" in str_link:
            #print(str_link)
            usa_links.append(str_link)
           # time.sleep(1)

# print(usa_links)

# get a count of how many links pulled used for reading only that amount of rows in the db
stop_count = len(usa_links)
# print(stop_count)


# traverse the db stop_count amount of rows to store links we already have into dblinks
c.execute('SELECT links FROM offers ORDER BY deal_id DESC LIMIT ' + str(stop_count))
for row in (c):
    tupString = ' '.join(map(str, row))
    dblinks.append(tupString)
#print(dblinks)

# set links already in db with new links so that there is not any duplicates and only search new links, we can use this to only grab links we haven't seen
set_new_links = set(usa_links).difference(dblinks)
# print(list(set_new_links))


# only add new links to the db and allow us to only search the new links to grab more info
for newlink in list(set_new_links):
    #if newlink not in ('http://www.fly4free.com/flight-deals/usa/non-stop-from-miami-to-aruba-for-195/'):
     #   continue
    r = requests.get(newlink)
    html_doc = r.text
    soup = BeautifulSoup(html_doc, "html.parser")

# do all the scraping of title, fares, dates, departure, destination, etc.
    # title
    for title in soup.findAll('title'):
        titles.append(str(soup.title.string))

    # dates - not always in the same place - loop through 3 different 'br' to find the data with months in it
    count = 0
    while count < 4:
        try:
            dates = (soup.select('div.article br')[count].next_sibling)
            if dates is None:
                break
        except IndexError:
            pass
        if any (x in dates for x in months):
            #print(dates)
            dates_string = dates.replace(u'\xa0', u' ')
            dates_list.append(dates_string)
            break
        else:
            #dates_list.append('No Dates')
            count = count + 1


# push everything to the DB
for newlink, title, daterange in zip((list(set_new_links)), titles, dates_list):
        try:
            c.execute("INSERT into offers (links, title) VALUES (?, ?)", (newlink, title))
            c.execute("INSERT into trip (dates) VALUES (?)", (daterange,))
            #print(newlink)
            #time.sleep(1)
        except sqlite3.IntegrityError:
            pass


print(dates_list)
print(type(titles))

conn.commit()
conn.close()


