--! qt:dataset:impala_dataset

EXPLAIN CBO PHYSICAL
SELECT
  f.`year`,
  a.iata,
  a.airport,
  a.city,
  a.state,
  a.country
FROM
  impala_flights f,
  impala_airports a
WHERE
  f.origin = a.iata
  AND f.`month` = month(current_date())
GROUP BY
  f.`year`,
  a.iata,
  a.airport,
  a.city,
  a.state,
  a.country
HAVING COUNT(*) > 10000
ORDER BY APPX_MEDIAN(f.DepDelay) DESC
LIMIT 10;
