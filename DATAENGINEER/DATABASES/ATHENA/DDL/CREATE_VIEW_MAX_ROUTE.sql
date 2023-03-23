CREATE OR REPLACE VIEW MOST_USED_ROUTE_BY_AIRCOMPANY AS

WITH max_icao_flight AS (

SELECT 
icao_empr,
num_voo,
"icaoaeródromoorigem",
"icaoaeródromodestino"

FROM (

SELECT

"númerovoo" as num_voo,
"icaoempresaaérea" as icao_empr,
"icaoaeródromoorigem",
"icaoaeródromodestino",
COUNT("númerovoo") as num_voo_count,
RANK() OVER(PARTITION BY "icaoempresaaérea" ORDER BY COUNT("númerovoo") DESC) Rank 

FROM objective_aerodromo.vra
WHERE "situaçãovoo" = 'REALIZADO'
GROUP BY
"icaoempresaaérea",
"númerovoo",
"icaoaeródromoorigem",
"icaoaeródromodestino"

ORDER BY num_voo_count DESC

)

WHERE Rank=1

),

icao_flight_filter_max_air_name AS (

SELECT

max_icao_flight.icao_empr,
max_icao_flight."icaoaeródromoorigem",
max_icao_flight."icaoaeródromodestino",
air_cia.razao_social

FROM max_icao_flight

LEFT JOIN air_cia

ON max_icao_flight.icao_empr = air_cia.icao

),

icao_flight_filter_max_air_name_origem AS (

SELECT 

icao_flight_filter_max_air_name.razao_social,
icao_flight_filter_max_air_name."icaoaeródromoorigem",
icao_name_list.name AS "name_aeroporto_origem",
icao_name_list.state AS "estado_aeroporto_origem",
icao_flight_filter_max_air_name."icaoaeródromodestino"

FROM icao_flight_filter_max_air_name

LEFT JOIN icao_name_list

ON icao_flight_filter_max_air_name."icaoaeródromoorigem" = icao_name_list.icao

),

final_table_view AS (

SELECT
icao_flight_filter_max_air_name_origem.*,
icao_name_list.name AS "name_aeroporto_destino",
icao_name_list.state AS "estado_aeroporto_destino"

FROM icao_flight_filter_max_air_name_origem

LEFT JOIN icao_name_list

ON

icao_flight_filter_max_air_name_origem."icaoaeródromodestino" = icao_name_list.icao

)

SELECT DISTINCT * FROM final_table_view;