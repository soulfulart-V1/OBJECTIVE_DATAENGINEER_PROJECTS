import os
import json
import boto3
import requests
import datetime

from io import StringIO
from pandas import DataFrame

#global itens
s3 = boto3.client('s3')
bucket_name = 'objective-data-lake'

def lambda_handler(event, context):

    year = event['time'][0:4]
    month = event['time'][5:7]

    date_to_extract =  datetime.datetime(year, month, day)
    
    #date to extract
    today_date_minus_one = date_to_extract - datetime.timedelta(days=1)
    
    #get year month and day separated
    used_year = str(today_date_minus_one.year)
    
    #avoid use numbers without left zero 
    if (today_date_minus_one.month < 10):
        used_month = '0'+str(today_date_minus_one.month)
        
    else: used_month = str(today_date_minus_one.month)

    object_path = 'RAW/DEPARTMENT_AERODROMO/PROJECT_445798_AIRLINE-VRA-AIR/VRA/'
    partition='yearmonth='
    objetct_key_prefix = object_path+partition+used_year+used_month
    
    filter_files = s3_client.list_objects(Bucket=bucket_name, Prefix=objetct_key_prefix)
    
    file_content = ""
    
    for item in filter_files['Contents']:
        file_content = file_content + str(s3_client.get_object(Bucket=bucket_name, Key=item['Key'])["Body"].read())

    air_json_data = json.loads(file_content)
    
    return {

        'message' : str(air_json_data)
        
    }