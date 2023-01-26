import os
import pytz
import json
import boto3
import requests
import datetime

from io import StringIO
from pandas import DataFrame

#global itens
s3_client = boto3.client('s3')
bucket_name = 'objective-data-lake'

def lambda_handler(event, context):

    year = int(event['time'][0:4])
    month = int(event['time'][5:7])
    day = 1 #it can be any day on the month

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
        file_content = file_content + s3_client.get_object(Bucket=bucket_name, Key=item['Key'])["Body"].read().decode('utf-8-sig')
        

    vra_data_dict = json.loads(file_content)
    vra_dataframe = DataFrame.from_dict(vra_data_dict)
    
    output_path = objetct_key_prefix.replace('RAW', 'CURATED')
    output_path = output_path+'/'+used_year+used_month+'.csv'
    
    vra_dataframe['transformed_date'] = datetime.datetime.now(pytz.timezone('America/Sao_Paulo'))
    data_csv_buffer = StringIO()
    vra_dataframe.to_csv(data_csv_buffer, index=False)

    s3_client.put_object(Body=data_csv_buffer.getvalue(), Bucket=bucket_name, Key=output_path)
    
    return {

        'air_json' : str(vra_dataframe.head()),
        'file_content_size' : str(len(file_content)/1e6),
        'file_content' : str(type(file_content))
        
    }