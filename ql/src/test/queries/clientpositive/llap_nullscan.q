--! qt:dataset:srcpart
set hive.mapred.mode=nonstrict;
set hive.cbo.enable=false;
set hive.explain.user=false;
set hive.fetch.task.conversion=none;
set hive.auto.convert.join=false;

set hive.vectorized.execution.enabled=true;
set hive.llap.io.enabled=true;
 
drop table if exists src_orc_n1;

create table src_orc_n1 stored as orc as select * from srcpart limit 10;

explain extended
select * from src_orc_n1 where 1=2;
select * from src_orc_n1 where 1=2;

explain
select * from (select key from src_orc_n1 where false) a left outer join (select key from src_orc_n1 limit 0) b on a.key=b.key;
select * from (select key from src_orc_n1 where false) a left outer join (select key from src_orc_n1 limit 0) b on a.key=b.key;
 
explain
select count(key) from src_orc_n1 where false union all select count(key) from src_orc_n1 ;
select count(key) from src_orc_n1 where false union all select count(key) from src_orc_n1 ;

explain 
select * from src_orc_n1 s1, src_orc_n1 s2 where false and s1.value = s2.value;
select * from src_orc_n1 s1, src_orc_n1 s2 where false and s1.value = s2.value;

drop table if exists src_orc_n1;
