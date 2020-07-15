--! qt:dataset:impala_dataset

EXPLAIN CBO
SELECT
  a.description,
  o.airport,
  MAX(f.DepDelay) AS MaxDepDelay
FROM
  impala_flights f,
  impala_airports o,
  impala_airlines a
WHERE
  f.origin = o.iata
  AND f.uniquecarrier = a.code
GROUP BY
  a.code,
  a.description,
  o.iata,
  o.airport
HAVING MAX(f.DepDelay) > ALL (SELECT CAST(AVG(DepDelay) AS BIGINT)
                              FROM impala_flights
                              GROUP BY impala_flights.origin)
ORDER BY MaxDepDelay DESC
LIMIT 50;
