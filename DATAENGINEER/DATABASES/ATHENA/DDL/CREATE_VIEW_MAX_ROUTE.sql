SELECT emp_max_count.icao_empr, emp_max_count.max_voo_count, emp_voo_count.num_voo

FROM

--table emp_max_count
(

SELECT icao_empr, MAX(num_voo_count) AS max_voo_count FROM  (
SELECT
"icaoempresaaérea" as icao_empr,
"númerovoo" as num_voo,
COUNT("númerovoo") as num_voo_count
FROM objective_aerodromo.vra
AS emp_voo_count
GROUP BY "icaoempresaaérea", "númerovoo"
ORDER BY num_voo_count DESC

)

GROUP BY icao_empr

) emp_max_count

LEFT JOIN

--emp_voo_count
(
SELECT
"icaoempresaaérea" as icao_empr,
"númerovoo" as num_voo,
COUNT("númerovoo") as num_voo_count
FROM objective_aerodromo.vra
AS emp_voo_count
GROUP BY "icaoempresaaérea", "númerovoo"
ORDER BY num_voo_count DESC
) emp_voo_count

ON

(emp_max_count.max_voo_count = emp_voo_count.num_voo_count)