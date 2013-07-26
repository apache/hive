set hive.ptf.partition.persistence.memsize=32;

--HIVE-4932 PTFOperator fails resetting PTFPersistence

SELECT
  key,
  value,
  ntile(10)
OVER (
  PARTITION BY value
  ORDER BY key DESC
)
FROM src;
