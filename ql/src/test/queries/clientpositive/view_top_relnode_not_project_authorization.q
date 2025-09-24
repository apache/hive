set hive.security.authorization.enabled=true;
create table t1 (username string);

create view vw_t1 as select distinct username from t1 limit 5;
explain cbo select * from vw_t1;
select * from vw_t1; 
