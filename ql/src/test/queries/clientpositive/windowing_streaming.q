--! qt:dataset:part
--! qt:dataset:alltypesorc
drop table over10k_n20;

create table over10k_n20(
           t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
	   ts timestamp, 
           `dec` decimal(4,2),  
           bin binary)
       row format delimited
       fields terminated by '|';

load data local inpath '../../data/files/over10k' into table over10k_n20;

set hive.limit.pushdown.memory.usage=.8;

-- part tests
explain
select * 
from ( select p_mfgr, rank() over(partition by p_mfgr order by p_name) r from part) a 
;

explain
select * 
from ( select p_mfgr, rank() over(partition by p_mfgr order by p_name) r from part) a 
where r < 4;

select * 
from ( select p_mfgr, rank() over(partition by p_mfgr order by p_name) r from part) a 
where r < 4;

select * 
from ( select p_mfgr, rank() over(partition by p_mfgr order by p_name) r from part) a 
where r < 2;

-- over10k_n20 tests
select * 
from (select t, f, rank() over(partition by t order by f) r from over10k_n20) a 
where r < 6 and t < 5;

select *
from (select t, f, row_number() over(partition by t order by f) r from over10k_n20) a
where r < 8 and t < 0;

set hive.vectorized.execution.enabled=false;
set hive.limit.pushdown.memory.usage=0.8;

explain
select * from (select ctinyint, cdouble, rank() over(partition by ctinyint order by cdouble) r from  alltypesorc) a where r < 5;

drop table if exists sB_n0;
create table sB_n0 ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  STORED AS TEXTFILE as  
select * from (select ctinyint, cdouble, rank() over(partition by ctinyint order by cdouble) r from  alltypesorc) a where r < 5;

select * from sB_n0
where ctinyint is null;

set hive.vectorized.execution.enabled=true;
set hive.limit.pushdown.memory.usage=0.8;
drop table if exists sD_n0;
create table sD_n0 ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  STORED AS TEXTFILE as  
select * from (select ctinyint, cdouble, rank() over(partition by ctinyint order by cdouble) r from  alltypesorc) a where r < 5;

select * from sD_n0
where ctinyint is null;
