# -*- coding: utf-8 

import pandas as pd
import requests
from bs4 import BeautifulSoup
import psycopg2
import random
'''
extraction process:
    - scrape NCAA Division 1 school data from wiki table and put it in a dataframe
    - import csv files (NCAA Division 1 school academic performance/sport contact data) and put in dataframes
'''

url = "https://en.wikipedia.org/wiki/List_of_NCAA_Division_I_institutions"
response = requests.get(url)

soup = BeautifulSoup(response.text, 'html.parser')
school_location_table = soup.find('table', {'class': "wikitable"})

temp_df = pd.read_html(str(school_location_table))
school_location_df = pd.DataFrame(temp_df[0])


school_academic_perf_df = pd.read_csv('NCAA_school_academic_performance.csv')

contact_sports_df = pd.read_csv('contact_sports.csv')



'''
transformation process:
    school_academic_performance_df
        - sport is listed in format "mens/womens sportname", seperate into 2 columns (gender, sport)
        - change gender column from 'men's/women's' to 'M/F'
        - different columns for each years data, convert to one year column with additional rows for each year
        - rename columns
        - drop unecessary data (school id, sport code, ncaa division (since all division 1))
'''

school_academic_perf_df[['gender', 'sport']] = school_academic_perf_df['SPORT_NAME'].str.split(' ', 1, expand = True)

#if the sport_name column doesnt have Men's/Women's as a prefix, then the sport name would be placed in gender, leaving sport empty
school_academic_perf_df.loc[school_academic_perf_df['sport'].isnull(), 'sport'] = school_academic_perf_df['gender']
school_academic_perf_df.loc[school_academic_perf_df['sport'] ==  school_academic_perf_df['gender'], 'gender'] = 'M'   
 
school_academic_perf_df.loc[school_academic_perf_df['gender'] ==  "Men's", 'gender'] = 'M'
school_academic_perf_df.loc[school_academic_perf_df['gender'] ==  "Women's", 'gender'] = 'F'


year_list = [2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014]
sap_red_df = pd.DataFrame()

for i in range(len(year_list)):
    temp_df = school_academic_perf_df[['SCHOOL_NAME', 'sport', 'gender', str(year_list[i]) + '_ATHLETES', str(year_list[i]) + '_SCORE']]
    temp_df = temp_df.rename(columns = {'SCHOOL_NAME':'school_name', str(year_list[i]) + '_ATHLETES' : "num_athletes", str(year_list[i]) + '_SCORE': "score"})
    temp_df['year'] = year_list[i]
    sap_red_df = sap_red_df.append(temp_df)
    
sap_red_df.reset_index(inplace=True)


#school_location_df.to_excel('wikitable.xlsx')

'''
load process:
    - configure the data into dataframes based on the created dimensional model
    - insert dataframes into respective postgresql database
'''
i = 0
surrogate_key_list = random.sample(range(10000, 99999), 1000)



date_dim = pd.DataFrame(sap_red_df['year'].drop_duplicates().reset_index(drop=True))
date_dim.loc[:, 'date_key'] = 0

for j in range(len(date_dim['date_key'])):
    date_dim['date_key'][j] = surrogate_key_list[i]
    i+=1
    
location_dim = pd.DataFrame(school_location_df[['State', 'City']].drop_duplicates().reset_index(drop=True))
location_dim.loc[:,'location_key'] = 0
for j in range(len(location_dim['location_key'])):
    location_dim['location_key'][j] = surrogate_key_list[i]
    i+=1

school_dim = pd.DataFrame(sap_red_df['school_name'].drop_duplicates().reset_index(drop=True))
school_dim.loc[:,'school_key'] = 0
for j in range(len(school_dim['school_key'])):
    school_dim['school_key'][j] = surrogate_key_list[i]
    i+=1

sport_dim = pd.DataFrame(sap_red_df[['gender', 'sport']].drop_duplicates().reset_index(drop=True))
sport_dim.loc[:,'sport_key'] = 0
for j in range(len(sport_dim['sport_key'])):
    sport_dim['sport_key'][j] = surrogate_key_list[i]
    i+=1
    
    
date_dim.to_excel('date_dim.xlsx')    
location_dim.to_excel('loc_dim.xlsx')    
school_dim.to_excel('school_dim.xlsx')
sport_dim.to_excel('sport_dim.xlsx')

db_name = "student_ath_academics"
username = "postgres"
password = "your_pass"

conn = psycopg2.connect(host = "localhost",
                        dbname = db_name,
                        user = username,
                        password = password)

cur = conn.cursor()

insert_test = """INSERT INTO student_ath.date_dim (date_year) VALUES (2005);"""
#cur.execute(insert_test)
#conn.commit()

#cur.execute("SELECT * FROM student_ath.date_dim;")
#print(cur.fetchall())

if(conn):
    conn.close()
    cur.close()






