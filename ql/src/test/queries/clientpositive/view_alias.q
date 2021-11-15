--! qt:dataset:src
drop view v_n6;
create view v_n6 as select 10 - key, '12' from src;
desc formatted v_n6;
select * from v_n6 order by `_c0`, `_c1` limit 5;

drop view v_n6;
create view v_n6 as select key as `_c1`, '12' from src;
desc formatted v_n6;
select * from v_n6 order by `_c1` limit 5;

drop view v_n6;
create view v_n6 as select *, '12' from src;
desc formatted v_n6;
select * from v_n6 order by key, value, `_c2` limit 5;

drop view v_n6;
create view v_n6 as select *, '12' as `_c121` from src;
desc formatted v_n6;
select * from v_n6 order by key, value, `_c121` limit 5;

drop view v_n6;
create view v_n6 as select key, count(*) from src group by key;
desc formatted v_n6;
select * from v_n6 order by key, `_c1` limit 5;


drop view v_n6;
create table a_n9 (ca_n9 string, caa_n9 string);
create table b_n7 (cb_n7 string, cbb_n7 string);
insert into a_n9 select * from src order by key, value limit 5;
insert into b_n7 select * from src order by key, value limit 5;
create view v_n6 as select '010', a_n9.*, 121, b_n7.*, 234 from a_n9 join b_n7 on a_n9.ca_n9 = b_n7.cb_n7;
desc formatted v_n6;
select * from v_n6 order by `_c3`, `_c0`, ca_n9, caa_n9, cb_n7, cbb_n7 limit 5;
