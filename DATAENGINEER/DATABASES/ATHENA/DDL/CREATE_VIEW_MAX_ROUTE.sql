CREATE OR REPLACE VIEW MOST_USED_ROUTE_BY_AIRCOMPANY AS

WITH max_icao_flight AS (

SELECT icao_empr, num_voo FROM (

SELECT

"icaoempresaaérea" as icao_empr,
"númerovoo" as num_voo,
COUNT("númerovoo") as num_voo_count,
RANK() OVER(PARTITION BY "icaoempresaaérea" ORDER BY COUNT("númerovoo") DESC) Rank 

FROM objective_aerodromo.vra
WHERE "situaçãovoo" = 'REALIZADO'
GROUP BY "icaoempresaaérea", "númerovoo"
ORDER BY num_voo_count DESC

)

WHERE Rank=1

),

icao_flight_filter_max AS (

SELECT DISTINCT

"icaoempresaaérea" as icao_empr,
"icaoaeródromoorigem",
"icaoaeródromodestino"

FROM objective_aerodromo.vra

WHERE "númerovoo" in (SELECT num_voo FROM max_icao_flight)

),

icao_flight_filter_max_air_name AS (

SELECT

icao_flight_filter_max.icao_empr,
icao_flight_filter_max."icaoaeródromoorigem",
icao_flight_filter_max."icaoaeródromodestino",
air_cia.razao_social

FROM icao_flight_filter_max

LEFT JOIN air_cia

ON icao_flight_filter_max.icao_empr = air_cia.icao

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

SELECT * FROM final_table_view;