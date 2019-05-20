create table t1 (a int) ;
insert into t1 values (1);

create table t2a  as 
        select * from t1
        union all
        select * from t1
;

select 2,count(*) from t2a;

create table t2b  as select * from
(
        select * from (select * from t1) sq1
        union all
        select * from (select * from t1) sq2
) tt
;


select 2,count(*) from t2b;

drop table if exists t1;
drop table if exists t2a;
drop table if exists t2b;

set hive.merge.tezfiles=true;

create table t1 (a int) stored as orc;
insert into t1 values (1);

analyze table t1 compute statistics for columns;

create table t2a stored as orc as
	select * from t1
	union all
	select * from t1
;

select 2,count(*) from t2a;

create table t2b stored as orc as select * from
(
	select * from (select * from t1) sq1
	union all
	select * from (select * from t1) sq2
) tt
;


select 2,count(*) from t2b;
