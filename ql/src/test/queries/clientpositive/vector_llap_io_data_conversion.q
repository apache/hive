--! qt:dataset:alltypesorc
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;

set hive.llap.io.enabled=true;
set hive.llap.io.encode.enabled=true;

create table varchar_single_partition(vt varchar(10), vsi varchar(10), vi varchar(20), vb varchar(30), vf varchar(20),vd varchar(20),vs varchar(50))
    partitioned by(s varchar(50)) stored as orc;
insert into table varchar_single_partition partition(s='positive') select ctinyint,csmallint,cint,cbigint,cfloat,cdouble,cstring1 from alltypesorc where cint>0 limit 10;
insert into table varchar_single_partition partition(s='negative') select ctinyint,csmallint,cint,cbigint,cfloat,cdouble,cstring1 from alltypesorc where cint<0 limit 10;
alter table varchar_single_partition change column vs vs varchar(10);

create table varchar_ctas_1 stored as orc as select vs, length(vs) as c1,reverse(vs) as c2 from varchar_single_partition where s='positive';

explain vectorization detail
select * from varchar_ctas_1 order by vs, c1, c2;

select * from varchar_ctas_1 order by vs, c1, c2;