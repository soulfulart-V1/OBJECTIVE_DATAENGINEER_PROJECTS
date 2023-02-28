import boto3
import urllib
import datetime

from io import StringIO
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
        file_content = file_content + s3_client.get_object(Bucket=bucket_name, Key=item['Key'])["Body"].read().decode('utf-8-sig')
        
    file_content_columned = []
    
    for item in file_content.split("\n"):
        add_file = item.split(";")
        file_content_columned.append(add_file)
    
    data_air_cia = DataFrame(file_content_columned, columns=file_content_columned[0])
    
    columns_snake = to_snake(list(data_air_cia.columns))

    #map old columns new columns
    map_columns = {}

    for key in data_air_cia.columns:
        for value in columns_snake:
            map_columns[key] = value
            columns_snake.remove(value)
            break

    data_air_cia = data_air_cia.rename(columns=map_columns)
    
    data_air_cia[['icao', 'iata']] = data_air_cia['icao_iata'].str.split(' ', 1, expand=True)
    data_air_cia.drop(columns=['icao_iata'], inplace=True)

    data_air_cia['icao'] = data_air_cia['icao'].replace("", None)
    data_air_cia['iata'] = data_air_cia['iata'].replace("", None)
    
    data_air_cia = data_air_cia[data_air_cia['razão_social']!='Razão Social']

    output_path = objetct_key_prefix.replace('RAW', 'CURATED')
    output_path = output_path+'/'+used_year+used_month+used_day+'.csv'
    
    data_csv_buffer = StringIO()
    data_air_cia.to_csv(data_csv_buffer, index=False)

    s3_client.put_object(Body=data_csv_buffer.getvalue(), Bucket=bucket_name, Key=output_path)

    return {

        'time' : str(list(data_air_cia.columns))
    }
    
def to_snake(string_list):
        
    i=0
    
    for item in string_list:
        string_list[i] = item.replace(" ", "_")
        string_list[i] = string_list[i].lower()
        i+=1
        
    return string_list