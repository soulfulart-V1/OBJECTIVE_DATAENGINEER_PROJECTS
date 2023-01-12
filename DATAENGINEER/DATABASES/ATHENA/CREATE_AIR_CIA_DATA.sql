CREATE EXTERNAL TABLE IF NOT EXISTS AERO.IOT_MESSAGES_REGION_A_BATCH (

    razão_social STRING,
    cnpj STRING,
    atividades_aéreas STRING,
    endereço_sede STRING,
    telefone STRING,
    e-mail STRING,
    decisão_operacional STRING,
    data_decisão_operacional STRING,
    validade_operacional STRING,
    icao STRING,
    iata STRING

    )

PARTITIONED BY(yearmonthday int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS CSV

LOCATION 's3://objective-data-lake/CURATED/DEPARTMENT_AERODROMO/PROJECT_445798_AIRLINE-VRA-AIR/AIR_CIA/'