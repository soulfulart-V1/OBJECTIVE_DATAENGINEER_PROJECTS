CREATE EXTERNAL TABLE IF NOT EXISTS OBJECTIVE_AERODROMO.AIR_CIA (
    razao_social STRING,
    cnpj STRING,
    atividades_aereas STRING,
    endereco_sede STRING,
    telefone STRING,
    email STRING,
    decisao_operacional STRING,
    data_decisao_operacional STRING,
    validade_operacional STRING,
    icao STRING,
    iata STRING
    )

PARTITIONED BY(yearmonthday int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','

LOCATION 's3://objective-data-lake/CURATED/DEPARTMENT_AERODROMO/PROJECT_445798_AIRLINE-VRA-AIR/AIR_CIA/'