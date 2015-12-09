set hive.mapred.mode=nonstrict;
CREATE TABLE src_bucket(key STRING, value STRING) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;


;

explain
insert into table src_bucket select key,value from srcpart;
insert into table src_bucket select key,value from srcpart;

select * from src_bucket limit 100;
