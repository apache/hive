drop view v;
create view v as select key, '12' from src;
desc formatted v;
select * from v order by `_c1` limit 5;

drop view v;
create view v as select key as `_c1`, '12' from src;
desc formatted v;
select * from v order by `_c1` limit 5;

drop view v;
create view v as select *, '12' from src;
desc formatted v;
select * from v order by `_c2` limit 5;

drop view v;
create view v as select *, '12' as `_c121` from src;
desc formatted v;
select * from v order by `_c121` limit 5;

drop view v;
create view v as select key, count(*) from src group by key;
desc formatted v;
select * from v order by `_c1` limit 5;


drop view v;
create table a (ca string, caa string);
create table b (cb string, cbb string);
insert into a select * from src limit 5;
insert into b select * from src limit 5;
create view v as select '010', a.*, 121, b.*, 234 from a join b on a.ca = b.cb;
desc formatted v;
select * from v order by `_c3` limit 5;
