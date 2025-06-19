import numpy as np  ## numpy is fast because it's core idea is written in c
## since it is fast, that's why finance companies use python (easy to code) with numpy (faster)
import pandas as pd #to work with tabular data
# we will be working wiht excel sheets

import requests  #for making http request to send it to API

import xlsxwriter #to write and edit directly the excel files

import math #ofcourse for math functions
'''import os

print("Current working directory:", os.getcwd())'''

stocks = pd.read_csv('equal_weight_S&P index fund/sp_500_stocks.csv')  #assign the stocks as pandas dataframe
# print(stocks)

from token_file import IEX_CLOUD_API_TOKEN   #imported API



