--! qt:dataset:part

set hive.vectorized.execution.enabled=false;
set hive.mapred.mode=nonstrict;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

-- SORT_QUERY_RESULTS

DROP TABLE parquet_partitioned_staging_temp;
DROP TABLE parquet_partitioned_temp;

CREATE TEMPORARY TABLE parquet_partitioned_staging_temp (
  id int,
  str string,
  part string
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|';

CREATE TEMPORARY TABLE parquet_partitioned_temp (
  id int,
  str string
) PARTITIONED BY (part string)
STORED AS PARQUET;

DESCRIBE FORMATTED parquet_partitioned_temp;

LOAD DATA LOCAL INPATH '../../data/files/parquet_partitioned.txt' OVERWRITE INTO TABLE parquet_partitioned_staging_temp;

SELECT * FROM parquet_partitioned_staging_temp;

INSERT OVERWRITE TABLE parquet_partitioned_temp PARTITION (part) SELECT * FROM parquet_partitioned_staging_temp;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SELECT * FROM parquet_partitioned_temp;
SELECT part, COUNT(0) FROM parquet_partitioned_temp GROUP BY part;

set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SELECT * FROM parquet_partitioned_temp;
SELECT part, COUNT(0) FROM parquet_partitioned_temp GROUP BY part;
