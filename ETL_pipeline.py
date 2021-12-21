# -*- coding: utf-8 -*-

import pandas as pd
import requests
from bs4 import BeautifulSoup

'''
extract process:
    - scrape NCAA Division 1 school data from wiki table and put it in a dataframe
    - import csv files (NCAA Division 1 school academic performance/city rent data) and put in dataframes
'''

url = "https://en.wikipedia.org/wiki/List_of_NCAA_Division_I_institutions"
response = requests.get(url)

soup = BeautifulSoup(response.text, 'html.parser')
school_location_table = soup.find('table', {'class': "wikitable"})

temp_df = pd.read_html(str(school_location_table))
school_location_df = pd.DataFrame(temp_df[0])
print(school_location_df.head())