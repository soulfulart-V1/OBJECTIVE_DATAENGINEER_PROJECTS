import json
import boto3
import urllib
import datetime
import requests

from pandas import DataFrame

#global objects
s3_client = boto3.client('s3')
bucket_name = 'objective-data-lake'

def lambda_handler(event, context):
    
    year = int(event['time'][0:4])
    month = int(event['time'][5:7])
    day = int(event['time'][8:10])

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

    object_path = 'CURATED/DEPARTMENT_AERODROMO/PROJECT_445798_AIRLINE-VRA-AIR/VRA/'
    partition='yearmonth=202101'
    objetct_key_prefix = object_path+partition
    
    filter_files = s3_client.list_objects(Bucket=bucket_name, Prefix=objetct_key_prefix)
    
    file_content = ""
    
    for item in filter_files['Contents']:
        file_content = file_content + s3_client.get_object(Bucket=bucket_name, Key=item['Key'])["Body"].read().decode('utf-8-sig')

    file_content_columned = []
    
    for item in file_content.split("\n"):
        add_file = item.split(",")
        file_content_columned.append(add_file)
    
    data_vra = DataFrame(file_content_columned, columns=file_content_columned[0])

    data_vra = data_vra[['ICAOAeródromoOrigem', 'ICAOAeródromoDestino']]

    data_vra.drop_duplicates(inplace = True)

    data_vra_group = DataFrame(columns=["All_ICAOS"])

    data_vra_group = data_vra_group.append(data_vra['ICAOAeródromoOrigem'], ignore_index=True)
    data_vra_group = data_vra_group.append(data_vra['ICAOAeródromoDestino'], ignore_index=True)

    del data_vra

    data_vra_group = data_vra_group[data_vra_group['All_ICAOS']!='ICAOAeródromoOrigem']
    data_vra_group = data_vra_group[data_vra_group['All_ICAOS']!='ICAOAeródromoDestino']
    data_vra_group = data_vra_group[data_vra_group['All_ICAOS']!='NaN']
    data_vra_group = data_vra_group[data_vra_group['All_ICAOS']!='None']

    return {
        
        'message' : str(data_vra_group.head())
        
    }