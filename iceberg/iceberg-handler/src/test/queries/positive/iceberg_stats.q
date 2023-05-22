--! qt:replace:/(\s+Statistics\: Num rows\: \d+ Data size\:\s+)\S+(\s+Basic stats\: \S+ Column stats\: \S+)/$1#Masked#$2/

set hive.compute.query.using.stats=true;
set hive.explain.user=false;

create table ice01 (id int, key int) Stored by Iceberg stored as ORC 
  TBLPROPERTIES('format-version'='2');

insert into ice01 values (1,1),(2,1),(3,1),(4,1),(5,1);
explain select count(*) from ice01;
select count(*) from ice01;

-- delete some values
delete from ice01 where id in (2,4);

explain select count(*) from ice01;
select count(*) from ice01;

-- iow
insert overwrite table ice01 select * from ice01;
explain select count(*) from ice01;

drop table ice01;