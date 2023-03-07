import os
import json
import boto3
import urllib
import datetime
import requests

from io import StringIO
from pandas import DataFrame

#global objects
s3_client = boto3.client('s3')
bucket_name = 'objective-data-lake'

def lambda_handler(event, context):
    
    year = 2023
    month = 3
    day = 7

    date_to_extract =  datetime.datetime(year, month, day)
    
    #get year month and day separated
    used_year = str(date_to_extract.year)
    
    #avoid use numbers without left zero 
    if (date_to_extract.month < 10):
        used_month = '0'+str(date_to_extract.month)
        
    else: used_month = str(date_to_extract.month)
    
    if (date_to_extract.day<10):
        used_day = '0'+str(date_to_extract.day)
    else: used_day = str(date_to_extract.day)

    object_path = 'CURATED/DEPARTMENT_AERODROMO/PROJECT_445798_AIRLINE-VRA-AIR/AERODROMOS_ICAO/'
    partition='yearmonthday='
    objetct_key_prefix = object_path+partition
    
    filter_files = s3_client.list_objects(Bucket=bucket_name, Prefix=objetct_key_prefix)

    last_icao_list = filter_files['Contents'][-1]

    file_content = s3_client.get_object(Bucket=bucket_name, Key=last_icao_list['Key'])["Body"].read().decode('utf-8-sig')

    last_icao_list = file_content.split('\n')

    url = "https://airport-info.p.rapidapi.com/airport"

    querystring = {"icao":"SBSV"}

    headers = {
        "X-RapidAPI-Key": os.environ['X_RapidAPI_Key'],
        "X-RapidAPI-Host": os.environ['X_RapidAPI_Host']        
        }

    response = requests.request("GET", url, headers=headers, params=querystring)

    return {
        
        'message' : str(response)
        
    }
