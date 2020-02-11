--! qt:dataset:src

set hive.vectorized.execution.enabled=false;

CREATE TABLE parquet_tbl(
  key int,
  ldate string)
 PARTITIONED BY (
 lyear string )
 ROW FORMAT SERDE
 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
 STORED AS INPUTFORMAT
 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
 OUTPUTFORMAT
 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat';

insert overwrite table parquet_tbl partition (lyear='2016') select
  1,
  '2016-02-03' from src limit 1;

set hive.optimize.ppd.storage = true;
set hive.optimize.ppd = true;
select * from parquet_tbl where ldate between '2016-02-03' and '2016-02-03';
drop table parquet_tbl;
