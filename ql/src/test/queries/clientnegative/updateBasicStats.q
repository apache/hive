set hive.mapred.mode=nonstrict;

create table s as select * from src limit 10;

alter table s update statistics set ('numRows'='NaN');
