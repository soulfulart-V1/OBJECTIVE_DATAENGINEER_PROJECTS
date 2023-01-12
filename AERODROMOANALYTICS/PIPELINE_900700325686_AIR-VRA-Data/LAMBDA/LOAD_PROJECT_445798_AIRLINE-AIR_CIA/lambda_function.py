import boto3
import urllib
import datetime

from pandas import DataFrame

#global objects
s3_client = boto3.client('s3') #define s3 service
bucket_name = 'objective-data-lake'

def lambda_handler(event, context):
    
    year = int(event['time'][0:4])
    month = int(event['time'][5:7])
    day = int(event['time'][8:10])

    date_to_extract =  datetime.datetime(year, month, day)
    
    #date to extract
    today_date_minus_one = date_to_extract - datetime.timedelta(days=1)
    
    #get year month and day separated
    used_year = str(today_date_minus_one.year)
    
    #avoid use numbers without left zero 
    if (today_date_minus_one.month < 10):
        used_month = '0'+str(today_date_minus_one.month)
        
    else: used_month = str(today_date_minus_one.month)
    
    if (today_date_minus_one.day<10):
        used_day = '0'+str(today_date_minus_one.day)
    else: used_day = str(today_date_minus_one.day)

    object_path = 'RAW/DEPARTMENT_AERODROMO/PROJECT_445798_AIRLINE-VRA-AIR/AIR_CIA/'
    partition='yearmonthday='
    objetct_key_prefix = object_path+partition+used_year+used_month+used_day
    
    filter_files = s3_client.list_objects(Bucket=bucket_name, Prefix=objetct_key_prefix)
    
    file_content = ""
    
    for item in filter_files['Contents']:
        file_content = file_content + str(s3_client.get_object(Bucket=bucket_name, Key=item['Key'])["Body"].read())
        
    file_content = (file_content.encode().decode('unicode_escape').encode('latin1').decode().lstrip('"b').strip("'"))
    file_content = file_content.replace("b'","")
    file_content = file_content.split('\n')
    
    columns = file_content[0].split(";")
    columns = to_snake(columns)
    
    file_content_list = []
    
    for item in file_content:
        file_content_list.append(item.split(";"))
        
    
    data_air_cia = DataFrame(file_content_list, columns=columns)
    
    data_air_cia[['icao', 'iata']] = data_air_cia['icao_iata'].str.split(' ', 1, expand=True)
    data_air_cia.drop(columns=['icao_iata'], inplace=True)

    data_air_cia['icao'] = data_air_cia['icao'].replace("", None)
    data_air_cia['iata'] = data_air_cia['iata'].replace("", None)
    
    output_path = objetct_key_prefix.replace('RAW', 'CURATED')
    output_path = output_path+'/'+used_year+used_month+used_day+'.csv'
    
    s3_client.put_object(Body=str(data_air_cia), Bucket=bucket_name, Key=output_path)

    return {
        'columns' : str(data_air_cia.columns),
        'data' : str(data_air_cia.head()),
        'time' : str(date_to_extract)        
    }
    
def to_snake(string_list):
        
    i=0
    
    for item in string_list:
        string_list[i] = item.replace(" ", "_")
        string_list[i] = string_list[i].lower()
        i+=1
        
    return string_list