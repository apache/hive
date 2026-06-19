set hive.security.authorization.enabled=true;
create table t1 (username string, id int);

create view vw_t0 as select distinct username from t1 group by username;
explain cbo select * from vw_t0;

create view vw_t1 as select distinct username from t1 order by username desc limit 5;
explain cbo select * from vw_t1;

create view vw_t2 as 
select username from (select username, id from t1 where id > 10 limit 1) x where username > 'a' order by id;
explain cbo select * from vw_t2;