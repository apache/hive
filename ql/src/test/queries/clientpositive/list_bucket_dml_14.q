--! qt:dataset:src
set hive.exec.dynamic.partition=true;
set hive.input.format=org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;
set mapred.input.dir.recursive=true;

-- list bucketing DML : unpartitioned table and 2 stage query plan.


-- create a skewed table
create table list_bucketing (key String, value String)
skewed by (key) on ("484")
stored as DIRECTORIES
;

-- list bucketing DML
explain extended
insert overwrite table list_bucketing select * from src;
insert overwrite table list_bucketing select * from src;

-- check DML result
desc formatted list_bucketing;

select count(1) from src;
select count(1) from list_bucketing;

select key, value from src where key = "484";
set hive.optimize.listbucketing=true;
explain extended
select key, value from list_bucketing where key = "484";
select key, value from list_bucketing where key = "484";

-- clean up resources
drop table list_bucketing;

