# -*- coding: utf-8 -*-
#import pytest
from ETL_pipeline import year_column_reformat
import pandas as pd

class Test_year_column_reformat:
    year_list = [2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014]
    orig_format_df = pd.DataFrame() 
    
    orig_format_df['SCHOOL_NAME'] = ["school1", "school2", "school2", "school3", "school3"]
    orig_format_df['sport'] = ["baseball", "swimming", "volleyball", "tennis", "baseball"]
    orig_format_df['gender'] = ["M", "M", "F", "M", "F"]

    for i in range(len(year_list)):
        orig_format_df[str(year_list[i]) + '_ATHLETES'] = [(i**2)%17, (i**3)%17, (i**4)%17, (i**5)%17, (i**6)%17]
        orig_format_df[str(year_list[i]) + '_SCORE'] = [(i**2)%99, (i**3)%99, (i**4)%99, (i**5)%99, (i**6)%99]

    new_format_df = year_column_reformat(year_list, orig_format_df)

    def test_num_columns(self):
        assert len(self.new_format_df.columns) == 7

    def test_num_rows(self):
        assert len(self.new_format_df) == 55
        
    def test_sum_athletes(self):
        assert self.new_format_df['num_athletes'].loc[self.new_format_df['year'] == 2006].sum() == self.orig_format_df["2006_ATHLETES"].sum()