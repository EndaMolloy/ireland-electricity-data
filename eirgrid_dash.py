# Adaptaded from scrapper at tmrowco/electricitymap-contrib

import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import requests
import gspread


def params_dict_2_query(query_params_dict, data_type='data'):
    query_root = f'http://smartgriddashboard.eirgrid.com/DashboardService.svc/{data_type}?'
    query_params = ''

    for query_param in query_params_dict.keys():
        query_param_val = query_params_dict[query_param]
        query_params += f'&{query_param}={query_param_val}'

    query = query_root + query_params
    return query

def create_query_params_dict(area, region, start_date, end_date):
    query_params_dict = dict()

    query_params_dict['area'] = area
    query_params_dict['region'] = region
    query_params_dict['datefrom'] = start_date
    query_params_dict['dateto'] = end_date

    return query_params_dict

def format_date_inputs(start_date, end_date):
    format_dt = lambda dt: datetime.strftime(dt, '%d-%b-%Y+%H:%M').replace(':', '%3A') if isinstance(dt, datetime) else dt

    start_date = format_dt(start_date)
    end_date = format_dt(end_date)

    return start_date, end_date

def query_API(start_date:datetime, end_date:datetime, area:str, region:str):
  start_date, end_date = format_date_inputs(start_date, end_date) # Format datetimes
  query_params_dict = create_query_params_dict(area, region, start_date, end_date) # create dictionary of parameters and their values
  query = params_dict_2_query(query_params_dict)

  ## Getting & Parsing Response
  response = requests.get(query)
  r_json = response.json()

  return r_json

def call_2_df(start_date, end_date, area, region, values):
    r_json = query_API(start_date, end_date, area, region) # Making Call
    df_raw = pd.DataFrame(r_json['Rows']) # Creating DataFrame
    
    if values == 'ALL':
      return df_raw
    else:
      return df_raw.dropna().tail(1)

def post_to_gspread(df):
  gc = gspread.service_account()
  sh = gc.open("IE_CO2_Intensity")
  df_list = df.values.tolist()
  sh.sheet1.append_row(df_list[0], table_range='A1')


## User Inputs
start_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
end_date = start_date + timedelta(days=1) - timedelta(minutes=1)
area = 'co2Intensity' # ['co2Intensity','co2Emission','pricing','frequency','interconnection', 'windforecast', 'windactual', 'generationactual', 'demandactual','marketdata']
region = 'ALL' # ['ALL', 'ROI', 'NI']
values = 'LAST' # ['ALL','LAST'] All rows or last row

df = call_2_df(start_date, end_date, area, region, values)
post_to_gspread(df)
# df.to_csv('IE_co2intensity.csv')

