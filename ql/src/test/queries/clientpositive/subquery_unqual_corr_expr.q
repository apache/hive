--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.optimize.shared.work.dppunion=false;

explain 
select * from src tablesample (10 rows) where lower(key) in (select key from src);
select * from src tablesample (10 rows) where lower(key) in (select key from src);

explain 
select * from src tablesample (10 rows) where concat(key,value) not in (select key from src);
select * from src tablesample (10 rows) where concat(key,value) not in (select key from src);
