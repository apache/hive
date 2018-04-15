--! qt:dataset:src1
--! qt:dataset:src
--! qt:dataset:alltypesorc
set hive.stats.column.autogather=false;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set tez.cartesian-product.max-parallelism=1;
-- Hybrid Grace Hash Join
-- Test basic functionalities:
-- 1. Various cases when hash partitions spill
-- 2. Partitioned table spilling
-- 3. Vectorization


SELECT 1;

set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask.size=1300000;
set hive.mapjoin.optimized.hashtable.wbsize=880000;
set hive.mapjoin.hybridgrace.memcheckfrequency=1024;

set hive.mapjoin.hybridgrace.hashtable=false;

-- Base result for inner join
explain
select count(*) from
(select c.ctinyint
 from alltypesorc c
 inner join alltypesorc cd
 on cd.cint = c.cint
 where c.cint < 2000000000) t1
;

select count(*) from
(select c.ctinyint
 from alltypesorc c
 inner join alltypesorc cd
 on cd.cint = c.cint
 where c.cint < 2000000000) t1
;

set hive.mapjoin.hybridgrace.hashtable=true;

-- Two partitions are created. One in memory, one on disk on creation.
-- The one in memory will eventually exceed memory limit, but won't spill.
explain
select count(*) from
(select c.ctinyint
 from alltypesorc c
 inner join alltypesorc cd
 on cd.cint = c.cint
 where c.cint < 2000000000) t1
;

select count(*) from
(select c.ctinyint
 from alltypesorc c
 inner join alltypesorc cd
 on cd.cint = c.cint
 where c.cint < 2000000000) t1
;

set hive.auto.convert.join.noconditionaltask.size=3000000;
set hive.mapjoin.optimized.hashtable.wbsize=100000;

set hive.mapjoin.hybridgrace.hashtable=false;

-- Base result for inner join
explain
select count(*) from
(select c.ctinyint
 from alltypesorc c
 inner join alltypesorc cd
 on cd.cint = c.cint) t1
;

select count(*) from
(select c.ctinyint
 from alltypesorc c
 inner join alltypesorc cd
 on cd.cint = c.cint) t1
;

set hive.mapjoin.hybridgrace.hashtable=true;

-- 16 partitions are created: 3 in memory, 13 on disk on creation.
-- 1 partition is spilled during first round processing, which ends up having 2 in memory, 14 on disk
explain
select count(*) from
(select c.ctinyint
 from alltypesorc c
 inner join alltypesorc cd
 on cd.cint = c.cint) t1
;

select count(*) from
(select c.ctinyint
 from alltypesorc c
 inner join alltypesorc cd
 on cd.cint = c.cint) t1
;



set hive.mapjoin.hybridgrace.hashtable=false;

-- Base result for outer join
explain
select count(*) from
(select c.ctinyint
 from alltypesorc c
 left outer join alltypesorc cd
 on cd.cint = c.cint) t1
;

select count(*) from
(select c.ctinyint
 from alltypesorc c
 left outer join alltypesorc cd
 on cd.cint = c.cint) t1
;

set hive.mapjoin.hybridgrace.hashtable=true;

-- 32 partitions are created. 3 in memory, 29 on disk on creation.
explain
select count(*) from
(select c.ctinyint
 from alltypesorc c
 left outer join alltypesorc cd
 on cd.cint = c.cint) t1
;

select count(*) from
(select c.ctinyint
 from alltypesorc c
 left outer join alltypesorc cd
 on cd.cint = c.cint) t1
;

set hive.llap.enable.grace.join.in.llap=true;

-- Partitioned table
create table parttbl (key string, value char(20)) partitioned by (dt char(10));
insert overwrite table parttbl partition(dt='2000-01-01')
  select * from src;
insert overwrite table parttbl partition(dt='2000-01-02')
  select * from src1;

set hive.auto.convert.join.noconditionaltask.size=30000000;
set hive.mapjoin.optimized.hashtable.wbsize=10000000;

set hive.mapjoin.hybridgrace.hashtable=false;

-- No spill, base result
explain
select count(*) from
(select p1.value
 from parttbl p1
 inner join parttbl p2
 on p1.key = p2.key) t1
;

select count(*) from
(select p1.value
 from parttbl p1
 inner join parttbl p2
 on p1.key = p2.key) t1
;

set hive.mapjoin.hybridgrace.hashtable=true;

-- No spill, 2 partitions created in memory
explain
select count(*) from
(select p1.value
 from parttbl p1
 inner join parttbl p2
 on p1.key = p2.key) t1
;

select count(*) from
(select p1.value
 from parttbl p1
 inner join parttbl p2
 on p1.key = p2.key) t1
;


set hive.auto.convert.join.noconditionaltask.size=20000;
set hive.mapjoin.optimized.hashtable.wbsize=10000;

set hive.mapjoin.hybridgrace.hashtable=false;

-- Spill case base result
explain
select count(*) from
(select p1.value
 from parttbl p1
 inner join parttbl p2
 on p1.key = p2.key) t1
;

select count(*) from
(select p1.value
 from parttbl p1
 inner join parttbl p2
 on p1.key = p2.key) t1
;

set hive.mapjoin.hybridgrace.hashtable=true;

-- Spill case, one partition in memory, one spilled on creation
explain
select count(*) from
(select p1.value
 from parttbl p1
 inner join parttbl p2
 on p1.key = p2.key) t1
;

select count(*) from
(select p1.value
 from parttbl p1
 inner join parttbl p2
 on p1.key = p2.key) t1
;

drop table parttbl;


-- Test vectorization
-- Test case borrowed from vector_decimal_mapjoin.q
CREATE TABLE decimal_mapjoin STORED AS ORC AS
  SELECT cdouble, CAST (((cdouble*22.1)/37) AS DECIMAL(20,10)) AS cdecimal1,
  CAST (((cdouble*9.3)/13) AS DECIMAL(23,14)) AS cdecimal2,
  cint
  FROM alltypesorc;

SET hive.auto.convert.join=true;
SET hive.auto.convert.join.noconditionaltask=true;
SET hive.auto.convert.join.noconditionaltask.size=50000000;
set hive.mapjoin.optimized.hashtable.wbsize=10000;
SET hive.vectorized.execution.enabled=true;
set hive.mapjoin.hybridgrace.hashtable=false;

EXPLAIN SELECT l.cint, r.cint, l.cdecimal1, r.cdecimal2
  FROM decimal_mapjoin l
  JOIN decimal_mapjoin r ON l.cint = r.cint
  WHERE l.cint = 6981;
SELECT l.cint, r.cint, l.cdecimal1, r.cdecimal2
  FROM decimal_mapjoin l
  JOIN decimal_mapjoin r ON l.cint = r.cint
  WHERE l.cint = 6981;

set hive.mapjoin.hybridgrace.hashtable=true;

EXPLAIN SELECT l.cint, r.cint, l.cdecimal1, r.cdecimal2
  FROM decimal_mapjoin l
  JOIN decimal_mapjoin r ON l.cint = r.cint
  WHERE l.cint = 6981;
SELECT l.cint, r.cint, l.cdecimal1, r.cdecimal2
  FROM decimal_mapjoin l
  JOIN decimal_mapjoin r ON l.cint = r.cint
  WHERE l.cint = 6981;

DROP TABLE decimal_mapjoin;
