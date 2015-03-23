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
