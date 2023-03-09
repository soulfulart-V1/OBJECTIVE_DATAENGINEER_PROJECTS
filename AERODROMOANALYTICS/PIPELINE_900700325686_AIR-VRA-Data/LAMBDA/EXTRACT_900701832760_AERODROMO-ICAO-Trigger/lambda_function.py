import os
import json
import boto3
import urllib
import datetime
import requests

from time import sleep
from pandas import DataFrame

#global objects
s3_client = boto3.client('s3')
bucket_name = 'objective-data-lake'
time_sleep = 0.1
failure_find_string = -1

def lambda_handler(event, context):
    
    year = 2023
    month = 3
    day = 9

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

    #new vra data arrived
    object_path = 'CURATED/DEPARTMENT_AERODROMO/PROJECT_445798_AIRLINE-VRA-AIR/VRA/'
    partition='yearmonth='
    objetct_key_prefix = object_path+partition
    
    filter_files = s3_client.list_objects(Bucket=bucket_name, Prefix=objetct_key_prefix)
    
    file_content = ""
    
    icao_new_arrived = DataFrame(columns=["All_ICAOS"])

    last_item = filter_files['Contents'][-1]
    
    file_content = s3_client.get_object(Bucket=bucket_name,\
                                        Key=last_item['Key'])["Body"].read().decode('utf-8-sig')
    
    file_content = file_content.split('\n')
    
    file_content_columned = []

    for item in file_content:
        file_content_columned.append(item.split(','))
    
    data_vra = DataFrame(file_content_columned, columns=file_content_columned[0])

    data_vra = data_vra[['ICAOAeródromoOrigem', 'ICAOAeródromoDestino']]
    
    data_vra.drop_duplicates(inplace = True)

    data_vra_aux = DataFrame(columns=["All_ICAOS"])
    data_vra_aux["All_ICAOS"] = data_vra['ICAOAeródromoOrigem']

    data_vra_aux_destino = DataFrame(columns=["All_ICAOS"])
    data_vra_aux_destino["All_ICAOS"] = data_vra['ICAOAeródromoDestino']

    data_vra_aux.append(data_vra_aux_destino, ignore_index=True)

    icao_new_arrived = icao_new_arrived.append(data_vra_aux, ignore_index=True)

    del data_vra
    del data_vra_aux
    del data_vra_aux_destino

    icao_new_arrived.drop_duplicates(inplace = True)
    icao_new_arrived = icao_new_arrived[icao_new_arrived['All_ICAOS']!='ICAOAeródromoOrigem']
    icao_new_arrived = icao_new_arrived[icao_new_arrived['All_ICAOS']!='ICAOAeródromoDestino']
    icao_new_arrived = icao_new_arrived[icao_new_arrived['All_ICAOS']!='NaN']
    icao_new_arrived = icao_new_arrived[icao_new_arrived['All_ICAOS']!='None']
    
    #up to date list
    icao_list = 'CURATED/DEPARTMENT_AERODROMO/PROJECT_445798_AIRLINE-VRA-AIR/AERODROMOS_ICAO/'
    partition='yearmonthday='
    objetct_key_prefix = icao_list + partition
    
    filter_files = s3_client.list_objects(Bucket=bucket_name, Prefix=objetct_key_prefix)

    last_icao_list = filter_files['Contents'][-1]

    file_content = s3_client.get_object(Bucket=bucket_name, Key=last_icao_list['Key'])["Body"].read().decode('utf-8-sig')

    last_icao_list = file_content.split('\n')
    new_get_icao = last_icao_list

    url = "https://airport-info.p.rapidapi.com/airport"
    icao_data_json = ""

    #drop already found icao
    for item in icao_new_arrived:
        new_get_icao.remove(item)

    icao_data_fail = []
    icao_name_csv = 'icao,name'
    
    for icao in last_icao_list:

        querystring = {"icao":icao}
        headers = {
        "X-RapidAPI-Key": os.environ['X_RapidAPI_Key'],
        "X-RapidAPI-Host": os.environ['X_RapidAPI_Host']        
        }
        
        response_text = requests.request("GET", url, headers=headers,\
                                         params=querystring).text

        if response_text.find('error') != failure_find_string:
            icao_data_fail.append(icao)

        else:
            #raw data
            icao_data_json = icao_data_json + response_text
            icao_data_dict = json.loads(response_text)

            #filtered name
            icao_name_csv = icao_name_csv + '\n' + icao + ',' + icao_data_dict['name']

        sleep(time_sleep)
        
    output_path = objetct_key_prefix.replace('CURATED', 'RAW')
    partition_value = used_year+used_month+used_day
    output_path = output_path + partition_value + '/' + partition_value+'.json'
    s3_client.put_object(Body=icao_data_json, Bucket=bucket_name, Key=output_path)

    output_path_with_name = 'CURATED/DEPARTMENT_AERODROMO/PROJECT_445798_AIRLINE-VRA-AIR/'
    output_path_with_name = output_path_with_name + 'AERODROMOS_ICAO_NAME/'
    output_path_with_name = output_path_with_name + partition + partition_value
    output_path_with_name = output_path_with_name + '/' + partition_value + '.csv'

    s3_client.put_object(Body=icao_name_csv,\
                         Bucket=bucket_name,\
                            Key=output_path_with_name)

    return {
        
        'message' : str(new_get_icao)
        
    }