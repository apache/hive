set hive.cli.print.header=true;
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
set hive.vectorized.execution.ptf.enabled=true;

CREATE TABLE repro_ptf_decimal (
  id STRING,
  total DECIMAL(20,2)
) STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB');

INSERT INTO repro_ptf_decimal VALUES ('req1', 5.0), ('req1', 10.0), ('req2', 10.0);

SELECT
  id,
  total,
  SUM(total + total + total) OVER (ORDER BY total DESC, id ASC) AS total_sum
FROM repro_ptf_decimal;
