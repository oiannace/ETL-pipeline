# -*- coding: utf-8 -*-
import psycopg2
import random
from pandasql import sqldf
import sqlalchemy
import ETL_pipeline
import pandas as pd
'''
load process:
    - configure the data into dataframes based on the created dimensional model
    - insert dataframes into respective postgresql database
'''
i = 0
surrogate_key_list = random.sample(range(10000, 99999), 1000)

sap_red_df, school_location_df, contact_sports_df = ETL_pipeline.loader_data()

date_dim = pd.DataFrame(sap_red_df['year'].drop_duplicates().reset_index(drop=True))
date_dim.loc[:, 'date_key'] = 0

for j in range(len(date_dim['date_key'])):
    date_dim['date_key'][j] = surrogate_key_list[i]
    i+=1
    
location_dim = pd.DataFrame(school_location_df[['state', 'city', 'school_name']].drop_duplicates().reset_index(drop=True))
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
    
    
school_dim_q ='''
    SELECT sc.school_key,
            sc.school_name,
            si.school_conference,
            si.school_type
    FROM school_dim sc
    INNER JOIN school_location_df si
    ON sc.school_name = si.school_name;
'''    
school_dim = sqldf(school_dim_q)

sport_dim_q = '''
    SELECT sp.sport_key,
            sp.gender,
            sp.sport,
            c.contact_sport
    FROM sport_dim sp
    INNER JOIN contact_sports_df c
    ON sp.sport = c.sport;
'''
sport_dim = sqldf(sport_dim_q)

#creating fact table from dimension tables and original raw table
fact_tbl_query = '''
    SELECT d.date_key, 
           l.location_key, 
           sc.school_key, 
           sp.sport_key, 
           st.num_athletes, 
           st.academic_score
           
    FROM sap_red_df st
    
    INNER JOIN date_dim d
    ON st.year = d.year
    
    INNER JOIN location_dim l
    ON st.school_name = l.school_name
    
    INNER JOIN school_dim sc
    ON st.school_name = sc.school_name
    
    INNER JOIN sport_dim sp
    ON st.gender = sp.gender
    AND st.sport = sp.sport;
'''

academic_score_snapshot_fact = sqldf(fact_tbl_query)

location_dim = location_dim.drop('school_name', axis=1)
'''
Loading dimension/fact tables into PostgreSQL database
'''

#USERNAME and PASSWORD are specific to your postgreSQL account
engine = sqlalchemy.create_engine("postgresql://username:password@localhost/student_ath_academics")

tables = [date_dim, location_dim, school_dim, sport_dim, academic_score_snapshot_fact]
table_names = ["date_dim", "location_dim", "school_dim", "sport_dim", "academic_score_snapshot_fact"]
for i in range(len(tables)):
    tables[i].to_sql(name=table_names[i], con=engine, schema="student_ath", if_exists="append", index=False)


