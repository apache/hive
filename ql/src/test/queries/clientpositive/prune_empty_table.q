create table tmp1 (a int) stored as orc;
create table tmp2 (a int) stored as orc;
insert into table tmp1 values (3);

explain cbo 
select * from tmp1 join tmp2 using(a);
select * from tmp1 join tmp2 using(a);

explain cbo
select count(*) from tmp2;
select count(*) from tmp2;

set hive.explain.prune.empty.table=true;

explain cbo 
select * from tmp1 join tmp2 using(a);
select * from tmp1 join tmp2 using(a);

explain cbo
select count(*) from tmp2;
select count(*) from tmp2;
