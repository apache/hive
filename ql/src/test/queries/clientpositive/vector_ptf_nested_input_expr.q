set hive.cli.print.header=true;
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
set hive.vectorized.execution.ptf.enabled=true;

CREATE TABLE repro_ptf_decimal (
  id STRING,
  val1 DECIMAL(20,2),
  val2 BIGINT,
  val3 SMALLINT
) STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB');

INSERT INTO repro_ptf_decimal VALUES ('req1', 5.0, 2, 1), ('req1', 10.0, 4, 2), ('req2', 10.0, 5, 2);

EXPLAIN VECTORIZATION DETAIL
SELECT
  id,
  val1,
  val2,
  val3,
  SUM(val1 * 2 + val2 - val3) OVER (ORDER BY val1 DESC, id ASC) AS final_total
FROM repro_ptf_decimal;

SELECT
  id,
  val1,
  val2,
  val3,
  SUM(val1 * 2 + val2 - val3) OVER (ORDER BY val1 DESC, id ASC) AS final_total
FROM repro_ptf_decimal;

CREATE TABLE repro_ptf_long (
  id STRING,
  val1 BIGINT,
  val2 INT,
  val3 SMALLINT
) STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB');

INSERT INTO repro_ptf_long VALUES ('req1', 5, 2, 1), ('req1', 10, 4, 2), ('req2', 10, 5, 2);

EXPLAIN VECTORIZATION DETAIL
SELECT
  id,
  val1,
  val2,
  val3,
  SUM(val1 * 2 + val2 - val3) OVER (ORDER BY val1 DESC, id ASC) AS final_total
FROM repro_ptf_long;

SELECT
  id,
  val1,
  val2,
  val3,
  SUM(val1 * 2 + val2 - val3) OVER (ORDER BY val1 DESC, id ASC) AS final_total
FROM repro_ptf_long;
