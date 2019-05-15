set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.blobstore.optimizations.enabled=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapreduce.input.fileinputformat.split.maxsize=10;
SET hive.merge.mapfiles=true;
set hive.optimize.sort.dynamic.partition=false;

CREATE TABLE tmp_table_merge (id string, name string, dt string, pid int);

INSERT INTO tmp_table_merge values ('u1','name1','2017-04-10',10000), ('u2','name2','2017-04-10',10000), ('u3','name3','2017-04-10',10000), ('u4','name4','2017-04-10',10001), ('u5','name5','2017-04-10',10001);

CREATE EXTERNAL TABLE s3_table_merge (user_id string, event_name string) PARTITIONED BY (reported_date string, product_id int) LOCATION '${hiveconf:test.blobstore.path.unique}/s3_table_merge/';

INSERT OVERWRITE TABLE s3_table_merge PARTITION (reported_date, product_id)
SELECT
  t.id as user_id,
  t.name as event_name,
  t.dt as reported_date,
  t.pid as product_id
FROM tmp_table_merge t;

select * from s3_table_merge order by user_id;

SET hive.blobstore.optimizations.enabled=false;

INSERT OVERWRITE TABLE s3_table_merge PARTITION (reported_date, product_id)
SELECT
  t.id as user_id,
  t.name as event_name,
  t.dt as reported_date,
  t.pid as product_id
FROM tmp_table_merge t;

select * from s3_table_merge order by user_id;

DROP TABLE s3_table_merge;
DROP TABLE tmp_table_merge;