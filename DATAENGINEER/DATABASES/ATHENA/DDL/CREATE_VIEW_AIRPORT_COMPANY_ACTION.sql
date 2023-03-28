CREATE OR REPLACE VIEW AIRPORT_COMPANY_ACTION AS

WITH company_airport_departures AS (

SELECT

"icaoaeródromoorigem",
"icaoempresaaérea" as icao_empr,
count("icaoempresaaérea") as rotas_partindo,
RANK() OVER(PARTITION BY "icaoaeródromoorigem" ORDER BY count("icaoempresaaérea") DESC) Rank 

FROM objective_aerodromo.vra
WHERE "situaçãovoo" = 'REALIZADO'
GROUP BY
"icaoaeródromoorigem",
"icaoempresaaérea"

),

company_airport_arrives AS (

SELECT

"icaoaeródromodestino",
"icaoempresaaérea" as icao_empr,
count("icaoempresaaérea") as rotas_chegando,
RANK() OVER(PARTITION BY "icaoaeródromodestino" ORDER BY count("icaoempresaaérea") DESC) Rank 

FROM objective_aerodromo.vra
WHERE "situaçãovoo" = 'REALIZADO'
GROUP BY
"icaoaeródromodestino",
"icaoempresaaérea"

),

all_departure_arrive AS (

SELECT

company_airport_departures."icaoaeródromoorigem",
company_airport_departures.icao_empr,
company_airport_departures.rotas_partindo,
company_airport_arrives.rotas_chegando,
company_airport_departures.rotas_partindo + company_airport_arrives.rotas_chegando as total_dept_arrive,
RANK() OVER(PARTITION BY 
company_airport_departures."icaoaeródromoorigem" 
ORDER BY 
company_airport_departures.rotas_partindo + company_airport_arrives.rotas_chegando
DESC) Rank 

FROM company_airport_departures

LEFT JOIN 

company_airport_arrives

ON 

company_airport_departures."icaoaeródromoorigem" = company_airport_arrives."icaoaeródromodestino"

AND

company_airport_departures.icao_empr = company_airport_arrives.icao_empr

),

all_departure_arrive_rank1 AS (

SELECT
"icaoaeródromoorigem" as icao_aeroporto,
icao_empr,
rotas_partindo,
rotas_chegando,
total_dept_arrive

FROM all_departure_arrive WHERE RANK=1

),

max_dept_arrive_airport_name AS (

SELECT
icao_name_list.name,
all_departure_arrive_rank1.*


FROM all_departure_arrive_rank1

LEFT JOIN 

icao_name_list

ON

all_departure_arrive_rank1.icao_aeroporto = icao_name_list.icao

),

max_dept_arrive_airport_company_name AS (

SELECT 
max_dept_arrive_airport_name.name as airport_name,
max_dept_arrive_airport_name.icao_aeroporto,
air_cia.razao_social as razao_social_companhia_aerea,
max_dept_arrive_airport_name.rotas_partindo,
max_dept_arrive_airport_name.rotas_chegando,
max_dept_arrive_airport_name.total_dept_arrive

FROM max_dept_arrive_airport_name

LEFT JOIN

air_cia

ON

max_dept_arrive_airport_name.icao_empr =
air_cia.icao

)

SELECT DISTINCT * FROM max_dept_arrive_airport_company_name