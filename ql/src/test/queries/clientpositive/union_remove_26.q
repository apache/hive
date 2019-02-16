set hive.stats.column.autogather=false;
set hive.stats.autogather=true;

-- This is to test the union remove optimization with stats optimization

create table inputSrcTbl1(key string, val int) stored as textfile;
create table inputSrcTbl2(key string, val int) stored as textfile;
create table inputSrcTbl3(key string, val int) stored as textfile;

load data local inpath '../../data/files/T1.txt' into table inputSrcTbl1;
load data local inpath '../../data/files/T2.txt' into table inputSrcTbl2;
load data local inpath '../../data/files/T3.txt' into table inputSrcTbl3;

create table inputTbl1_n6(key string, val int) stored as textfile;
create table inputTbl2(key string, val int) stored as textfile;
create table inputTbl3(key string, val int) stored as textfile;

insert into inputTbl1_n6 select * from inputSrcTbl1;
insert into inputTbl2 select * from inputSrcTbl2;
insert into inputTbl3 select * from inputSrcTbl3;

set hive.compute.query.using.stats=true;
set hive.optimize.union.remove=true;

--- union remove optimization effects, stats optimization does not though it is on since inputTbl2 column stats is not available
analyze table inputTbl1_n6 compute statistics for columns;
analyze table inputTbl3 compute statistics for columns;
explain
  SELECT count(1) as rowcnt, min(val) as ms, max(val) as mx from inputTbl1_n6
  UNION ALL
  SELECT count(1) as rowcnt, min(val) as ms, max(val) as mx from inputTbl2
  UNION ALL
  SELECT count(1) as rowcnt, min(val) as ms, max(val) as mx from inputTbl3;


select count(*) from (
  SELECT count(1) as rowcnt, min(val) as ms, max(val) as mx from inputTbl1_n6
  UNION ALL
  SELECT count(1) as rowcnt, min(val) as ms, max(val) as mx from inputTbl2
  UNION ALL
  SELECT count(1) as rowcnt, min(val) as ms, max(val) as mx from inputTbl3) t;

--- union remove optimization and stats optimization are effective after inputTbl2 column stats is calculated
analyze table inputTbl2 compute statistics for columns;
explain
  SELECT count(1) as rowcnt, min(val) as ms, max(val) as mx from inputTbl1_n6
  UNION ALL
  SELECT count(1) as rowcnt, min(val) as ms, max(val) as mx from inputTbl2
  UNION ALL
  SELECT count(1) as rowcnt, min(val) as ms, max(val) as mx from inputTbl3;


select count(*) from (
  SELECT count(1) as rowcnt, min(val) as ms, max(val) as mx from inputTbl1_n6
  UNION ALL
  SELECT count(1) as rowcnt, min(val) as ms, max(val) as mx from inputTbl2
  UNION ALL
  SELECT count(1) as rowcnt, min(val) as ms, max(val) as mx from inputTbl3) t;

--- union remove optimization effects but stats optimization does not (with group by) though it is on
explain
  SELECT key, count(1) as rowcnt, min(val) as ms, max(val) as mx from inputTbl1_n6 group by key
  UNION ALL
  SELECT key, count(1) as rowcnt, min(val) as ms, max(val) as mx from inputTbl2 group by key
  UNION ALL
  SELECT key, count(1) as rowcnt, min(val) as ms, max(val) as mx from inputTbl3 group by key;

select count(*) from (
  SELECT key, count(1) as rowcnt, min(val) as ms, max(val) as mx from inputTbl1_n6 group by key
  UNION ALL
  SELECT key, count(1) as rowcnt, min(val) as ms, max(val) as mx from inputTbl2 group by key
  UNION ALL
  SELECT key, count(1) as rowcnt, min(val) as ms, max(val) as mx from inputTbl3 group by key) t;


set hive.compute.query.using.stats=false;
set hive.optimize.union.remove=true;

explain
  SELECT count(1) as rowcnt, min(val) as ms, max(val) as mx from inputTbl1_n6
  UNION ALL
  SELECT count(1) as rowcnt, min(val) as ms, max(val) as mx from inputTbl2
  UNION ALL
  SELECT count(1) as rowcnt, min(val) as ms, max(val) as mx from inputTbl3;

select count(*) from (
  SELECT count(1) as rowcnt, min(val) as ms, max(val) as mx from inputTbl1_n6
  UNION ALL
  SELECT count(1) as rowcnt, min(val) as ms, max(val) as mx from inputTbl2
  UNION ALL
  SELECT count(1) as rowcnt, min(val) as ms, max(val) as mx from inputTbl3) t;


set hive.compute.query.using.stats=false;
set hive.optimize.union.remove=false;

explain
  SELECT count(1) as rowcnt, min(val) as ms, max(val) as mx from inputTbl1_n6
  UNION ALL
  SELECT count(1) as rowcnt, min(val) as ms, max(val) as mx from inputTbl2
  UNION ALL
  SELECT count(1) as rowcnt, min(val) as ms, max(val) as mx from inputTbl3;


select count(*) from (
  SELECT count(1) as rowcnt, min(val) as ms, max(val) as mx from inputTbl1_n6
  UNION ALL
  SELECT count(1) as rowcnt, min(val) as ms, max(val) as mx from inputTbl2
  UNION ALL
  SELECT count(1) as rowcnt, min(val) as ms, max(val) as mx from inputTbl3) t;
