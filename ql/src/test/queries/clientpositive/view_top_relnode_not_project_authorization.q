set hive.security.authorization.enabled=true;
create table t1 (username string, id int);

create view vw_t1 as select distinct username from t1 limit 5;
explain cbo select * from vw_t1;
select * from vw_t1; 

create view vw_t2 as 
select username from (select username, id from t1 where id > 10 limit 1) x where username > 'a' order by id;
explain cbo select * from vw_t2;
select * from vw_t2;

create view vw_t3 as
select username from (select username, id from t1 where id > 10 limit 10) x where username > 'a' limit 5;
explain cbo select * from vw_t3;
select * from vw_t3;
