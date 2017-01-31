#This is just a script for how I exstracted useful (structured) data from unstructured data consisting of web pages.
#I am free to release the code, but not the data that this was used for.
#It also shows how data may be sent to an SQLite database from within python.


import json

#this is the code used to create the table containing the data output of the parsing.
#creates a new table in an existing database.
import sqlite3#this is the api and database system I will use in this project.
import csv

from unidecode import unidecode
def _removeNonAscii(s): return "".join(i for i in s if ord(i)<128)

#Creation of a new DB.
#Code is saved to remember process of setingup.
#Tutorial is at: http://sebastianraschka.com/Articles/2014_sqlite_in_python_tutorial.html#connecting-to-an-sqlite-database
sqlite_file = 'n_db.sqlite'    # name of the sqlite database file
# Committing changes and closing the connection to the database file
conn = sqlite3.connect(sqlite_file)
c = conn.cursor()
conn.commit()
conn.close()

# this can be used to remove the Z_SHOES table).
# I hope not to need this. but it is a sort of "restart" button. so the script runs the same first, second, and nth time...
conn = sqlite3.connect('n_db.sqlite')
cur = conn.cursor()
try:# to start from a clean slate at each execution I want to eliminate the data table and create it anew.
    cur.execute("DROP TABLE Z_SHOES;")
except:
    pass
conn.commit()
conn.close()

import csv
#reconds failure to input row to sql.
writer_SQL_f = csv.writer(open("fail_SQL.csv",'wb'))

#records failure to read a json file into readable data.
writer_json_f = csv.writer(open("fail_json.csv",'wb'))

conn = sqlite3.connect('n_db.sqlite')
print "Opened database successfully";
#these lines are commented out so as not to try to create a table that already exists.
#but it is still worth recording the steps used to make it.
conn.execute('''CREATE TABLE Z_SHOES
       (ID INT PRIMARY KEY     NOT NULL,/*arbratery ID variable*/
       BRANDNAME TEXT,/*brand name as on website*/
       PRODOCTNAME TEXT,/*prodoct name as given in website*/
       URL TEXT,/*This stores the URL of the page the prodect info was found on.*/
       PAGE INT,/* this is used to save the page number. For a given catagory*/
       PAGE_POPRANK INT,/* For each page, this is the rank shown. Lower Numbers (should) indicate higher popularity*/
       CATAGORY TEXT, /*Catagory as given on the website*/
       PRICE INT,
       TIME_ST TEXT,    /*string of timestamp*/
       URL_CATAGORY TEXT, /*url of site including catagory.*/
       URL_SITE TEXT/*this is the URL of the website only.*/);''')
print "Table (Z_SHOES) created successfully";
conn.commit()
conn.close()

#include something here about how to clear tables if desired.


max_row_id = 0
#we need to get the max ID value in the DB.
conn = sqlite3.connect('n_db.sqlite')
print "Opened database successfully";
cursor = conn.cursor()
cursor.execute('''SELECT ID, BRANDNAME, URL FROM Z_SHOES''')
for row in cursor:
    # row[0] returns the first column in the query (name), row[1] returns brand column.
    print "this is a row of the database:"
    print('{0} : {1}, {2}'.format(row[0], row[1], row[2]))
    row_id = ('{0}'.format(row[0]))
    row_id = int(row_id)
    if row_id > max_row_id:
        max_row_id = row_id
    else:
        pass
conn.commit()
conn.close()
# print "max_row_id: %i" %max_row_id

ID = max_row_id




x = 0
data = []

#RAW DATA - json lines file.
my_file = 'crawl_z.nl_2016-05-30T23-14-36.jl'

#lets just work with a subset of the data for now.
data = data[0:4]
# "data_temp" + str(i)
ld = len(data) - 1
r = range(0, ld) #range of page numbers to be read thru.

import re

import csv

#I am going to use some code from http://pandas.pydata.org/pandas-docs/version/0.18.1/generated/pandas.read_json.html
#it should give us 'json2csv.py'
# import unicode
import os

#atempt to use json to csv conversion on indevidual files.



#we are going to use beautiful soup to scan for what we need
from bs4 import BeautifulSoup #because I can.


#and we are also going to use regular expressions
import re
import json

ls_s_price = ls_prodoctName = ls_brandName = ls_url = ls_t_stamp = []
i = 0
with open(my_file) as f:
    for line in f:
        json_read = False #variable to record if data is successfully written to db
        data = json.loads(line)#reads line as data.
        # x = x + 1
        # for i in r:
        print( "reading json %i" %i)
        d = data
        i = i + 1
        n = "data_temp" + str(i) + ".json"

        print( "the keys are:-")
        ls_keys = list(d.keys())#should look like:
        #[u'body', u'crawled_at', u'product_category', u'ordering', u'page_number', u'page_type', u'page_url']
        print( ls_keys )

        body = d.get("body")
        print( d.get("page_url"))
        # print( d.get("body"))
        print( "d.get('body').__class__.__name__" )
        print( d.get("body").__class__.__name__ )#according to this it should be possible to parse through for the html.

        #this line is only intended to save the output strings for inspection and comparison.
        #these files are too big to see in a most tools. But it works with nano. D:
        # '''
        if i < 7:#this is only (currenly) being used to test the system
            with open(n, 'w') as outfile:
                json.dump(d, outfile)
            print( "%s has been created" %n)
            body_n = "body_" + n
            with open(body_n, 'w') as outfile:
                json.dump(body, outfile)
            print( "%s has been created" %body_n)
        elif i < 3000: #3000 loops takes about 10 minuets
            print "itteration: ", i
        else:
            break#this is temporary.
        # '''
        i = i + 1

        soup = BeautifulSoup(body)
        s_cont = soup.findAll("div", {"class": "catalogArticlesList_container"})#contains indevidual articlel seperately.
        if len(s_cont) < 1:#this line is in case there are no sub-sections to list indevidual items in sub sections in page.
            s_cont = [soup]


        p_rank = 0#keeps track of position in a given page, prodocts on each page are ranked by popularity. Hence saving rank on page tells us the reletive popularity of each entery.

        for s_art in s_cont:#this iterates through every shoe shown on the Z website.
            json_read = True # if it gets this far the json has been read successfully
            #z-shoes formats (discounted):
            s_price = [s_art.findAll("div", {"class": "catalogArticlesList_price specialPrice"})] + [s_art.findAll("div"), {"class": "zvui_price_info discounted"}] #z alternate formating of price.
            s_price = s_price[0]
            if len(s_price) < 1:
                s_price = [s_art.findAll("div", {"class": "catalogArticlesList_price"})]
                s_price = s_price[0]
            else:
                pass

            #to get the brand (it is formated as below)
            brandName = s_art.find("div", {"class": "catalogArticlesList_brandName"})
            prodoctName = s_art.find("div", {"class": "catalogArticlesList_articleName"})
            #and other info
            url = d.get("page_url")
            t_stamp = d.get("crawled_at")
            catagory = 'NOTYETSETUP'
            #row containin the releveant info for a database input.
            p_rank = p_rank + 1
            # s_price = _removeNonAscii(s_price)
            #we can now clean data to go into a table
            # s_price = unicode(s_price, 'utf8')
            # s_price = strip_non_ascii(s_price)
            s_price = str(s_price)#convert unicode into ascii
            s_price = s_price.replace('\u20ac', '')
            print "s_price2", s_price

            # s_price.replace('u20a', '') #the euro symbol if converted to ascii will look like this.
            c_price = re.sub('[^0-9]','', s_price)#remove non-numeric charichters.
            print "c_price", c_price
            try:
                c_price = int(c_price)
            except:
                c_price = None
                print "no c_price for ", url
            ID = ID + 1 #used to identify the posion of a row in the database.

            #now to use replace statement to remove non-relevant info
            #also, I need to remove all the non-ascii charichters.
            try:
                brandName = str(brandName)
                brandName = ''.join([x for x in brandName if ord(x) < 128]) #removes non-ascii
                brandName = brandName.replace('<div class="catalogArticlesList_brandName">', '')
                brandName = brandName.replace('</div>', '')
            except:
                brandName = None
                print "no brandName"
            try:
                prodoctName = str(prodoctName)
                prodoctName = ''.join([x for x in prodoctName if ord(x) < 128]) #removes non-ascii
                prodoctName = prodoctName.replace('<div class="catalogArticlesList_articleName">', '')
                prodoctName = prodoctName.replace('</div>', '')
            except:
                brandName = None
                print "no prodoctName"
            #finaly, to ensure no weird unicode:
            url = str(url)

            t_stamp = str(t_stamp)#to get timestamp prepared

            ls_url_site = re.findall('http.*\.nl/',url)#this will find the basic website url. i.e. name the site.
            url_site = ls_url_site[0]
            #we should then add the data as a row in the sqlite database.
            #as I am reletively new to sqlite -- https://www.tutorialspoint.com/sqlite/sqlite_python.htm
            conn = sqlite3.connect('n_db.sqlite')
            print "Opened database successfully";
            ls_pnum = re.findall('\&p=[0-9]*', url) #page number represented by a '$p=3'
            if len(ls_pnum) > 0:
                str_pnum = ls_pnum[0]
                str_pnum = str_pnum.replace("&p=", "")
                str_pnum = str_pnum.replace("?p=", "")
                pnum = int(str_pnum) #to select the numerals only
                url_catagory = url.replace(str_pnum, "") #to get the catagory listed in the URL. (defined as part of URL not refering to site, and not refering to page number)
                url_catagory = url_catagory.replace(str(url_site), "")
            else: #there is sometimes no actual p. number listed in the url. this means we are at page 1.
                pnum = 1
                url_catagory = url
                url_catagory = url_catagory.replace(str(url_site), "")
            page = pnum

            to_db = [ID, brandName, prodoctName, url, page, p_rank, catagory, c_price, t_stamp, url_catagory, url_site]#p_rank, c_price, prodoctName, brandName, url, t_stamp, catagory]
            cur = conn.cursor()
            #first we should make sure that there is not a row duplicate in the database already.
            #I have not yet added this feature.

            try:
                cur.execute("INSERT INTO Z_SHOES (ID,BRANDNAME,PRODOCTNAME,URL,PAGE,PAGE_POPRANK,CATAGORY,PRICE,TIME_ST,URL_CATAGORY,URL_SITE) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,?,?);", to_db)
                conn.commit()
                print "Records created successfully";
                conn.close()
                records_to_SQL = True #variable to record status of records input
            except:
                print "failure to write a record."
                print "the input:\n", to_db
                conn.close()
                #if this except statement is sometimes activated I need to work out why.
                records_to_SQL = False
            if records_to_SQL == False:
                writer_SQL_f.writerow(to_db) #write missing data to a .csv.
        if json_read == False:
            ls_k = []
            ls_keys_nobody = [i] + ls_keys[1:] #we do not want to include body. we want to include the itteration.

            for k in ls_keys_nobody:
                ls_k = ls_k + [d.get(k)]
            writer_json_f.writerow(ls_k)

writer_json_f.close()
writer_SQL_f.close()
print s_price
print "This is max_row_id already in DB at start of this script."
print max_row_id
print "This is the currunt maximum row ID at the end of script:"
print ID
print "price in cents:"
print c_price
print "\n"
print to_db
print pnum
