--! qt:dataset:src
set hive.explain.user=false;
set hive.fetch.task.conversion=none;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000;
SET hive.merge.nway.joins=false;

-- SORT_QUERY_RESULTS

create table t1_n148 stored as orc as select cast(key as int) key, value from src where key <= 10;

select * from t1_n148 sort by key;

create table t2_n87 stored as orc as select cast(2*key as int) key, value from t1_n148;

select * from t2_n87 sort by key;

create table t3_n35 stored as orc as select * from (select * from t1_n148 union all select * from t2_n87) b;
select * from t3_n35 sort by key, value;

analyze table t3_n35 compute statistics;

create table t4_n19 (key int, value string) stored as orc;
select * from t4_n19;


set hive.vectorized.execution.enabled=false;
set hive.mapjoin.hybridgrace.hashtable=false;

explain vectorization expression
select * from t1_n148 a left semi join t2_n87 b on a.key=b.key sort by a.key, a.value;
select * from t1_n148 a left semi join t2_n87 b on a.key=b.key sort by a.key, a.value;

explain vectorization expression
select * from t2_n87 a left semi join t1_n148 b on b.key=a.key sort by a.key, a.value;
select * from t2_n87 a left semi join t1_n148 b on b.key=a.key sort by a.key, a.value;

explain vectorization expression
select * from t1_n148 a left semi join t4_n19 b on b.key=a.key sort by a.key, a.value;
select * from t1_n148 a left semi join t4_n19 b on b.key=a.key sort by a.key, a.value;

explain vectorization expression
select a.value from t1_n148 a left semi join t3_n35 b on (b.key = a.key and b.key < '15') sort by a.value;
select a.value from t1_n148 a left semi join t3_n35 b on (b.key = a.key and b.key < '15') sort by a.value;

explain vectorization expression
select * from t1_n148 a left semi join t2_n87 b on a.key = b.key and b.value < "val_10" sort by a.key, a.value;
select * from t1_n148 a left semi join t2_n87 b on a.key = b.key and b.value < "val_10" sort by a.key, a.value;

explain vectorization expression
select a.value from t1_n148 a left semi join (select key from t3_n35 where key > 5) b on a.key = b.key sort by a.value;
select a.value from t1_n148 a left semi join (select key from t3_n35 where key > 5) b on a.key = b.key sort by a.value;

explain vectorization expression
select a.value from t1_n148 a left semi join (select key , value from t2_n87 where key > 5) b on a.key = b.key and b.value <= 'val_20' sort by a.value ;
select a.value from t1_n148 a left semi join (select key , value from t2_n87 where key > 5) b on a.key = b.key and b.value <= 'val_20' sort by a.value ;

explain vectorization expression
select * from t2_n87 a left semi join (select key , value from t1_n148 where key > 2) b on a.key = b.key sort by a.key, a.value;
select * from t2_n87 a left semi join (select key , value from t1_n148 where key > 2) b on a.key = b.key sort by a.key, a.value;

explain vectorization expression
select /*+ mapjoin(b) */ a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key sort by a.key;
select /*+ mapjoin(b) */ a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key sort by a.key;

explain vectorization expression
select * from t1_n148 a left semi join t2_n87 b on a.key = 2*b.key sort by a.key, a.value;
select * from t1_n148 a left semi join t2_n87 b on a.key = 2*b.key sort by a.key, a.value;

explain vectorization expression
select * from t1_n148 a join t2_n87 b on a.key = b.key left semi join t3_n35 c on b.key = c.key sort by a.key, a.value;
select * from t1_n148 a join t2_n87 b on a.key = b.key left semi join t3_n35 c on b.key = c.key sort by a.key, a.value;
 
explain vectorization expression
select * from t3_n35 a left semi join t1_n148 b on a.key = b.key and a.value=b.value sort by a.key, a.value;
select * from t3_n35 a left semi join t1_n148 b on a.key = b.key and a.value=b.value sort by a.key, a.value;

explain vectorization expression
select /*+ mapjoin(b, c) */ a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key left semi join t2_n87 c on a.key = c.key sort by a.key;
select /*+ mapjoin(b, c) */ a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key left semi join t2_n87 c on a.key = c.key sort by a.key;

explain vectorization expression
select a.key from t3_n35 a left outer join t1_n148 b on a.key = b.key left semi join t2_n87 c on b.key = c.key sort by a.key;
select a.key from t3_n35 a left outer join t1_n148 b on a.key = b.key left semi join t2_n87 c on b.key = c.key sort by a.key;

SET hive.mapjoin.full.outer=false;
explain vectorization expression
select a.key from t1_n148 a full outer join t3_n35 b on a.key = b.key left semi join t2_n87 c on b.key = c.key sort by a.key;
select a.key from t1_n148 a full outer join t3_n35 b on a.key = b.key left semi join t2_n87 c on b.key = c.key sort by a.key;

SET hive.mapjoin.full.outer=true;
explain vectorization expression
select a.key from t1_n148 a full outer join t3_n35 b on a.key = b.key left semi join t2_n87 c on b.key = c.key sort by a.key;
select a.key from t1_n148 a full outer join t3_n35 b on a.key = b.key left semi join t2_n87 c on b.key = c.key sort by a.key;

-- Verify this works (FULL OUTER MapJoin is not enabled for N-way)
SET hive.merge.nway.joins=true;
explain vectorization expression
select a.key from t1_n148 a full outer join t3_n35 b on a.key = b.key left semi join t2_n87 c on b.key = c.key sort by a.key;
select a.key from t1_n148 a full outer join t3_n35 b on a.key = b.key left semi join t2_n87 c on b.key = c.key sort by a.key;
SET hive.merge.nway.joins=false;

explain vectorization expression
select a.key from t3_n35 a left semi join t2_n87 b on a.key = b.key left outer join t1_n148 c on a.key = c.key sort by a.key;
select a.key from t3_n35 a left semi join t2_n87 b on a.key = b.key left outer join t1_n148 c on a.key = c.key sort by a.key;

SET hive.mapjoin.full.outer=false;
explain vectorization expression
select a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key full outer join t2_n87 c on a.key = c.key sort by a.key;
select a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key full outer join t2_n87 c on a.key = c.key sort by a.key;

SET hive.mapjoin.full.outer=true;
explain vectorization expression
select a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key full outer join t2_n87 c on a.key = c.key sort by a.key;
select a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key full outer join t2_n87 c on a.key = c.key sort by a.key;

explain vectorization expression
select a.key from t3_n35 a left semi join t2_n87 b on a.key = b.key left outer join t1_n148 c on a.value = c.value sort by a.key;
select a.key from t3_n35 a left semi join t2_n87 b on a.key = b.key left outer join t1_n148 c on a.value = c.value sort by a.key;

explain vectorization expression
select a.key from t3_n35 a left semi join t2_n87 b on a.value = b.value where a.key > 100;
select a.key from t3_n35 a left semi join t2_n87 b on a.value = b.value where a.key > 100;

set hive.vectorized.execution.enabled=false;
set hive.mapjoin.hybridgrace.hashtable=true;
set hive.llap.enable.grace.join.in.llap=true;

explain vectorization operator
select * from t1_n148 a left semi join t2_n87 b on a.key=b.key sort by a.key, a.value;
select * from t1_n148 a left semi join t2_n87 b on a.key=b.key sort by a.key, a.value;

explain vectorization operator
select * from t2_n87 a left semi join t1_n148 b on b.key=a.key sort by a.key, a.value;
select * from t2_n87 a left semi join t1_n148 b on b.key=a.key sort by a.key, a.value;

explain vectorization operator
select * from t1_n148 a left semi join t4_n19 b on b.key=a.key sort by a.key, a.value;
select * from t1_n148 a left semi join t4_n19 b on b.key=a.key sort by a.key, a.value;

explain vectorization operator
select a.value from t1_n148 a left semi join t3_n35 b on (b.key = a.key and b.key < '15') sort by a.value;
select a.value from t1_n148 a left semi join t3_n35 b on (b.key = a.key and b.key < '15') sort by a.value;

explain vectorization operator
select * from t1_n148 a left semi join t2_n87 b on a.key = b.key and b.value < "val_10" sort by a.key, a.value;
select * from t1_n148 a left semi join t2_n87 b on a.key = b.key and b.value < "val_10" sort by a.key, a.value;

explain vectorization operator
select a.value from t1_n148 a left semi join (select key from t3_n35 where key > 5) b on a.key = b.key sort by a.value;
select a.value from t1_n148 a left semi join (select key from t3_n35 where key > 5) b on a.key = b.key sort by a.value;

explain vectorization operator
select a.value from t1_n148 a left semi join (select key , value from t2_n87 where key > 5) b on a.key = b.key and b.value <= 'val_20' sort by a.value ;
select a.value from t1_n148 a left semi join (select key , value from t2_n87 where key > 5) b on a.key = b.key and b.value <= 'val_20' sort by a.value ;

explain vectorization operator
select * from t2_n87 a left semi join (select key , value from t1_n148 where key > 2) b on a.key = b.key sort by a.key, a.value;
select * from t2_n87 a left semi join (select key , value from t1_n148 where key > 2) b on a.key = b.key sort by a.key, a.value;

explain vectorization operator
select /*+ mapjoin(b) */ a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key sort by a.key;
select /*+ mapjoin(b) */ a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key sort by a.key;

explain vectorization operator
select * from t1_n148 a left semi join t2_n87 b on a.key = 2*b.key sort by a.key, a.value;
select * from t1_n148 a left semi join t2_n87 b on a.key = 2*b.key sort by a.key, a.value;

explain vectorization operator
select * from t1_n148 a join t2_n87 b on a.key = b.key left semi join t3_n35 c on b.key = c.key sort by a.key, a.value;
select * from t1_n148 a join t2_n87 b on a.key = b.key left semi join t3_n35 c on b.key = c.key sort by a.key, a.value;
 
explain vectorization operator
select * from t3_n35 a left semi join t1_n148 b on a.key = b.key and a.value=b.value sort by a.key, a.value;
select * from t3_n35 a left semi join t1_n148 b on a.key = b.key and a.value=b.value sort by a.key, a.value;

explain vectorization operator
select /*+ mapjoin(b, c) */ a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key left semi join t2_n87 c on a.key = c.key sort by a.key;
select /*+ mapjoin(b, c) */ a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key left semi join t2_n87 c on a.key = c.key sort by a.key;

explain vectorization operator
select a.key from t3_n35 a left outer join t1_n148 b on a.key = b.key left semi join t2_n87 c on b.key = c.key sort by a.key;
select a.key from t3_n35 a left outer join t1_n148 b on a.key = b.key left semi join t2_n87 c on b.key = c.key sort by a.key;

SET hive.mapjoin.full.outer=false;
explain vectorization operator
select a.key from t1_n148 a full outer join t3_n35 b on a.key = b.key left semi join t2_n87 c on b.key = c.key sort by a.key;
select a.key from t1_n148 a full outer join t3_n35 b on a.key = b.key left semi join t2_n87 c on b.key = c.key sort by a.key;

SET hive.mapjoin.full.outer=true;
explain vectorization operator
select a.key from t1_n148 a full outer join t3_n35 b on a.key = b.key left semi join t2_n87 c on b.key = c.key sort by a.key;
select a.key from t1_n148 a full outer join t3_n35 b on a.key = b.key left semi join t2_n87 c on b.key = c.key sort by a.key;

explain vectorization operator
select a.key from t3_n35 a left semi join t2_n87 b on a.key = b.key left outer join t1_n148 c on a.key = c.key sort by a.key;
select a.key from t3_n35 a left semi join t2_n87 b on a.key = b.key left outer join t1_n148 c on a.key = c.key sort by a.key;

SET hive.mapjoin.full.outer=false;
explain vectorization operator
select a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key full outer join t2_n87 c on a.key = c.key sort by a.key;
select a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key full outer join t2_n87 c on a.key = c.key sort by a.key;

SET hive.mapjoin.full.outer=true;
explain vectorization operator
select a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key full outer join t2_n87 c on a.key = c.key sort by a.key;
-- select a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key full outer join t2_n87 c on a.key = c.key sort by a.key;

explain vectorization operator
select a.key from t3_n35 a left semi join t2_n87 b on a.key = b.key left outer join t1_n148 c on a.value = c.value sort by a.key;
select a.key from t3_n35 a left semi join t2_n87 b on a.key = b.key left outer join t1_n148 c on a.value = c.value sort by a.key;

explain vectorization operator
select a.key from t3_n35 a left semi join t2_n87 b on a.value = b.value where a.key > 100;
select a.key from t3_n35 a left semi join t2_n87 b on a.value = b.value where a.key > 100;

set hive.vectorized.execution.enabled=true;
set hive.mapjoin.hybridgrace.hashtable=false;
SET hive.vectorized.execution.mapjoin.native.enabled=false;

explain vectorization only operator
select * from t1_n148 a left semi join t2_n87 b on a.key=b.key sort by a.key, a.value;
select * from t1_n148 a left semi join t2_n87 b on a.key=b.key sort by a.key, a.value;

explain vectorization only operator
select * from t2_n87 a left semi join t1_n148 b on b.key=a.key sort by a.key, a.value;
select * from t2_n87 a left semi join t1_n148 b on b.key=a.key sort by a.key, a.value;

explain vectorization only operator
select * from t1_n148 a left semi join t4_n19 b on b.key=a.key sort by a.key, a.value;
select * from t1_n148 a left semi join t4_n19 b on b.key=a.key sort by a.key, a.value;

explain vectorization only operator
select a.value from t1_n148 a left semi join t3_n35 b on (b.key = a.key and b.key < '15') sort by a.value;
select a.value from t1_n148 a left semi join t3_n35 b on (b.key = a.key and b.key < '15') sort by a.value;

explain vectorization only operator
select * from t1_n148 a left semi join t2_n87 b on a.key = b.key and b.value < "val_10" sort by a.key, a.value;
select * from t1_n148 a left semi join t2_n87 b on a.key = b.key and b.value < "val_10" sort by a.key, a.value;

explain vectorization only operator
select a.value from t1_n148 a left semi join (select key from t3_n35 where key > 5) b on a.key = b.key sort by a.value;
select a.value from t1_n148 a left semi join (select key from t3_n35 where key > 5) b on a.key = b.key sort by a.value;

explain vectorization only operator
select a.value from t1_n148 a left semi join (select key , value from t2_n87 where key > 5) b on a.key = b.key and b.value <= 'val_20' sort by a.value ;
select a.value from t1_n148 a left semi join (select key , value from t2_n87 where key > 5) b on a.key = b.key and b.value <= 'val_20' sort by a.value ;

explain vectorization only operator
select * from t2_n87 a left semi join (select key , value from t1_n148 where key > 2) b on a.key = b.key sort by a.key, a.value;
select * from t2_n87 a left semi join (select key , value from t1_n148 where key > 2) b on a.key = b.key sort by a.key, a.value;

explain vectorization only operator
select /*+ mapjoin(b) */ a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key sort by a.key;
select /*+ mapjoin(b) */ a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key sort by a.key;

explain vectorization only operator
select * from t1_n148 a left semi join t2_n87 b on a.key = 2*b.key sort by a.key, a.value;
select * from t1_n148 a left semi join t2_n87 b on a.key = 2*b.key sort by a.key, a.value;

explain vectorization only operator
select * from t1_n148 a join t2_n87 b on a.key = b.key left semi join t3_n35 c on b.key = c.key sort by a.key, a.value;
select * from t1_n148 a join t2_n87 b on a.key = b.key left semi join t3_n35 c on b.key = c.key sort by a.key, a.value;
 
explain vectorization only operator
select * from t3_n35 a left semi join t1_n148 b on a.key = b.key and a.value=b.value sort by a.key, a.value;
select * from t3_n35 a left semi join t1_n148 b on a.key = b.key and a.value=b.value sort by a.key, a.value;

explain vectorization only operator
select /*+ mapjoin(b, c) */ a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key left semi join t2_n87 c on a.key = c.key sort by a.key;
select /*+ mapjoin(b, c) */ a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key left semi join t2_n87 c on a.key = c.key sort by a.key;

explain vectorization only operator
select a.key from t3_n35 a left outer join t1_n148 b on a.key = b.key left semi join t2_n87 c on b.key = c.key sort by a.key;
select a.key from t3_n35 a left outer join t1_n148 b on a.key = b.key left semi join t2_n87 c on b.key = c.key sort by a.key;

SET hive.mapjoin.full.outer=false;
explain vectorization operator
select a.key from t1_n148 a full outer join t3_n35 b on a.key = b.key left semi join t2_n87 c on b.key = c.key sort by a.key;
select a.key from t1_n148 a full outer join t3_n35 b on a.key = b.key left semi join t2_n87 c on b.key = c.key sort by a.key;

SET hive.mapjoin.full.outer=true;
explain vectorization operator
select a.key from t1_n148 a full outer join t3_n35 b on a.key = b.key left semi join t2_n87 c on b.key = c.key sort by a.key;
-- select a.key from t1_n148 a full outer join t3_n35 b on a.key = b.key left semi join t2_n87 c on b.key = c.key sort by a.key;

explain vectorization only operator
select a.key from t3_n35 a left semi join t2_n87 b on a.key = b.key left outer join t1_n148 c on a.key = c.key sort by a.key;
select a.key from t3_n35 a left semi join t2_n87 b on a.key = b.key left outer join t1_n148 c on a.key = c.key sort by a.key;

SET hive.mapjoin.full.outer=false;
explain vectorization only operator
select a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key full outer join t2_n87 c on a.key = c.key sort by a.key;
select a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key full outer join t2_n87 c on a.key = c.key sort by a.key;

SET hive.mapjoin.full.outer=true;
explain vectorization only operator
select a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key full outer join t2_n87 c on a.key = c.key sort by a.key;
-- select a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key full outer join t2_n87 c on a.key = c.key sort by a.key;

explain vectorization only operator
select a.key from t3_n35 a left semi join t2_n87 b on a.key = b.key left outer join t1_n148 c on a.value = c.value sort by a.key;
select a.key from t3_n35 a left semi join t2_n87 b on a.key = b.key left outer join t1_n148 c on a.value = c.value sort by a.key;

explain vectorization only operator
select a.key from t3_n35 a left semi join t2_n87 b on a.value = b.value where a.key > 100;
select a.key from t3_n35 a left semi join t2_n87 b on a.value = b.value where a.key > 100;

set hive.vectorized.execution.enabled=true;
set hive.mapjoin.hybridgrace.hashtable=true;
set hive.llap.enable.grace.join.in.llap=true;
SET hive.vectorized.execution.mapjoin.native.enabled=false;

explain vectorization operator
select * from t1_n148 a left semi join t2_n87 b on a.key=b.key sort by a.key, a.value;
select * from t1_n148 a left semi join t2_n87 b on a.key=b.key sort by a.key, a.value;

explain vectorization operator
select * from t2_n87 a left semi join t1_n148 b on b.key=a.key sort by a.key, a.value;
select * from t2_n87 a left semi join t1_n148 b on b.key=a.key sort by a.key, a.value;

explain vectorization operator
select * from t1_n148 a left semi join t4_n19 b on b.key=a.key sort by a.key, a.value;
select * from t1_n148 a left semi join t4_n19 b on b.key=a.key sort by a.key, a.value;

explain vectorization operator
select a.value from t1_n148 a left semi join t3_n35 b on (b.key = a.key and b.key < '15') sort by a.value;
select a.value from t1_n148 a left semi join t3_n35 b on (b.key = a.key and b.key < '15') sort by a.value;

explain vectorization operator
select * from t1_n148 a left semi join t2_n87 b on a.key = b.key and b.value < "val_10" sort by a.key, a.value;
select * from t1_n148 a left semi join t2_n87 b on a.key = b.key and b.value < "val_10" sort by a.key, a.value;

explain vectorization operator
select a.value from t1_n148 a left semi join (select key from t3_n35 where key > 5) b on a.key = b.key sort by a.value;
select a.value from t1_n148 a left semi join (select key from t3_n35 where key > 5) b on a.key = b.key sort by a.value;

explain vectorization operator
select a.value from t1_n148 a left semi join (select key , value from t2_n87 where key > 5) b on a.key = b.key and b.value <= 'val_20' sort by a.value ;
select a.value from t1_n148 a left semi join (select key , value from t2_n87 where key > 5) b on a.key = b.key and b.value <= 'val_20' sort by a.value ;

explain vectorization operator
select * from t2_n87 a left semi join (select key , value from t1_n148 where key > 2) b on a.key = b.key sort by a.key, a.value;
select * from t2_n87 a left semi join (select key , value from t1_n148 where key > 2) b on a.key = b.key sort by a.key, a.value;

explain vectorization operator
select /*+ mapjoin(b) */ a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key sort by a.key;
select /*+ mapjoin(b) */ a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key sort by a.key;

explain vectorization operator
select * from t1_n148 a left semi join t2_n87 b on a.key = 2*b.key sort by a.key, a.value;
select * from t1_n148 a left semi join t2_n87 b on a.key = 2*b.key sort by a.key, a.value;

explain vectorization operator
select * from t1_n148 a join t2_n87 b on a.key = b.key left semi join t3_n35 c on b.key = c.key sort by a.key, a.value;
select * from t1_n148 a join t2_n87 b on a.key = b.key left semi join t3_n35 c on b.key = c.key sort by a.key, a.value;
 
explain vectorization operator
select * from t3_n35 a left semi join t1_n148 b on a.key = b.key and a.value=b.value sort by a.key, a.value;
select * from t3_n35 a left semi join t1_n148 b on a.key = b.key and a.value=b.value sort by a.key, a.value;

explain vectorization operator
select /*+ mapjoin(b, c) */ a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key left semi join t2_n87 c on a.key = c.key sort by a.key;
select /*+ mapjoin(b, c) */ a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key left semi join t2_n87 c on a.key = c.key sort by a.key;

explain vectorization operator
select a.key from t3_n35 a left outer join t1_n148 b on a.key = b.key left semi join t2_n87 c on b.key = c.key sort by a.key;
select a.key from t3_n35 a left outer join t1_n148 b on a.key = b.key left semi join t2_n87 c on b.key = c.key sort by a.key;

SET hive.mapjoin.full.outer=false;
explain vectorization operator
select a.key from t1_n148 a full outer join t3_n35 b on a.key = b.key left semi join t2_n87 c on b.key = c.key sort by a.key;
select a.key from t1_n148 a full outer join t3_n35 b on a.key = b.key left semi join t2_n87 c on b.key = c.key sort by a.key;

SET hive.mapjoin.full.outer=true;
explain vectorization operator
select a.key from t1_n148 a full outer join t3_n35 b on a.key = b.key left semi join t2_n87 c on b.key = c.key sort by a.key;
select a.key from t1_n148 a full outer join t3_n35 b on a.key = b.key left semi join t2_n87 c on b.key = c.key sort by a.key;

explain vectorization operator
select a.key from t3_n35 a left semi join t2_n87 b on a.key = b.key left outer join t1_n148 c on a.key = c.key sort by a.key;
select a.key from t3_n35 a left semi join t2_n87 b on a.key = b.key left outer join t1_n148 c on a.key = c.key sort by a.key;

SET hive.mapjoin.full.outer=false;
explain vectorization operator
select a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key full outer join t2_n87 c on a.key = c.key sort by a.key;
select a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key full outer join t2_n87 c on a.key = c.key sort by a.key;

SET hive.mapjoin.full.outer=true;
explain vectorization operator
select a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key full outer join t2_n87 c on a.key = c.key sort by a.key;
select a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key full outer join t2_n87 c on a.key = c.key sort by a.key;

explain vectorization operator
select a.key from t3_n35 a left semi join t2_n87 b on a.key = b.key left outer join t1_n148 c on a.value = c.value sort by a.key;
select a.key from t3_n35 a left semi join t2_n87 b on a.key = b.key left outer join t1_n148 c on a.value = c.value sort by a.key;

explain vectorization operator
select a.key from t3_n35 a left semi join t2_n87 b on a.value = b.value where a.key > 100;
select a.key from t3_n35 a left semi join t2_n87 b on a.value = b.value where a.key > 100;

set hive.vectorized.execution.enabled=true;
set hive.mapjoin.hybridgrace.hashtable=false;
SET hive.vectorized.execution.mapjoin.native.enabled=true;

explain vectorization operator
select * from t1_n148 a left semi join t2_n87 b on a.key=b.key sort by a.key, a.value;
select * from t1_n148 a left semi join t2_n87 b on a.key=b.key sort by a.key, a.value;

explain vectorization operator
select * from t2_n87 a left semi join t1_n148 b on b.key=a.key sort by a.key, a.value;
select * from t2_n87 a left semi join t1_n148 b on b.key=a.key sort by a.key, a.value;

explain vectorization operator
select * from t1_n148 a left semi join t4_n19 b on b.key=a.key sort by a.key, a.value;
select * from t1_n148 a left semi join t4_n19 b on b.key=a.key sort by a.key, a.value;

explain vectorization operator
select a.value from t1_n148 a left semi join t3_n35 b on (b.key = a.key and b.key < '15') sort by a.value;
select a.value from t1_n148 a left semi join t3_n35 b on (b.key = a.key and b.key < '15') sort by a.value;

explain vectorization operator
select * from t1_n148 a left semi join t2_n87 b on a.key = b.key and b.value < "val_10" sort by a.key, a.value;
select * from t1_n148 a left semi join t2_n87 b on a.key = b.key and b.value < "val_10" sort by a.key, a.value;

explain vectorization operator
select a.value from t1_n148 a left semi join (select key from t3_n35 where key > 5) b on a.key = b.key sort by a.value;
select a.value from t1_n148 a left semi join (select key from t3_n35 where key > 5) b on a.key = b.key sort by a.value;

explain vectorization operator
select a.value from t1_n148 a left semi join (select key , value from t2_n87 where key > 5) b on a.key = b.key and b.value <= 'val_20' sort by a.value ;
select a.value from t1_n148 a left semi join (select key , value from t2_n87 where key > 5) b on a.key = b.key and b.value <= 'val_20' sort by a.value ;

explain vectorization operator
select * from t2_n87 a left semi join (select key , value from t1_n148 where key > 2) b on a.key = b.key sort by a.key, a.value;
select * from t2_n87 a left semi join (select key , value from t1_n148 where key > 2) b on a.key = b.key sort by a.key, a.value;

explain vectorization operator
select /*+ mapjoin(b) */ a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key sort by a.key;
select /*+ mapjoin(b) */ a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key sort by a.key;

explain vectorization operator
select * from t1_n148 a left semi join t2_n87 b on a.key = 2*b.key sort by a.key, a.value;
select * from t1_n148 a left semi join t2_n87 b on a.key = 2*b.key sort by a.key, a.value;

explain vectorization operator
select * from t1_n148 a join t2_n87 b on a.key = b.key left semi join t3_n35 c on b.key = c.key sort by a.key, a.value;
select * from t1_n148 a join t2_n87 b on a.key = b.key left semi join t3_n35 c on b.key = c.key sort by a.key, a.value;
 
explain vectorization operator
select * from t3_n35 a left semi join t1_n148 b on a.key = b.key and a.value=b.value sort by a.key, a.value;
select * from t3_n35 a left semi join t1_n148 b on a.key = b.key and a.value=b.value sort by a.key, a.value;

explain vectorization operator
select /*+ mapjoin(b, c) */ a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key left semi join t2_n87 c on a.key = c.key sort by a.key;
select /*+ mapjoin(b, c) */ a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key left semi join t2_n87 c on a.key = c.key sort by a.key;

explain vectorization operator
select a.key from t3_n35 a left outer join t1_n148 b on a.key = b.key left semi join t2_n87 c on b.key = c.key sort by a.key;
select a.key from t3_n35 a left outer join t1_n148 b on a.key = b.key left semi join t2_n87 c on b.key = c.key sort by a.key;

SET hive.mapjoin.full.outer=false;
explain vectorization operator
select a.key from t1_n148 a full outer join t3_n35 b on a.key = b.key left semi join t2_n87 c on b.key = c.key sort by a.key;
select a.key from t1_n148 a full outer join t3_n35 b on a.key = b.key left semi join t2_n87 c on b.key = c.key sort by a.key;

SET hive.mapjoin.full.outer=true;
explain vectorization operator
select a.key from t1_n148 a full outer join t3_n35 b on a.key = b.key left semi join t2_n87 c on b.key = c.key sort by a.key;
select a.key from t1_n148 a full outer join t3_n35 b on a.key = b.key left semi join t2_n87 c on b.key = c.key sort by a.key;

explain vectorization operator
select a.key from t3_n35 a left semi join t2_n87 b on a.key = b.key left outer join t1_n148 c on a.key = c.key sort by a.key;
select a.key from t3_n35 a left semi join t2_n87 b on a.key = b.key left outer join t1_n148 c on a.key = c.key sort by a.key;

SET hive.mapjoin.full.outer=false;
explain vectorization operator
select a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key full outer join t2_n87 c on a.key = c.key sort by a.key;
select a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key full outer join t2_n87 c on a.key = c.key sort by a.key;

SET hive.mapjoin.full.outer=true;
explain vectorization operator
select a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key full outer join t2_n87 c on a.key = c.key sort by a.key;
select a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key full outer join t2_n87 c on a.key = c.key sort by a.key;

explain vectorization operator
select a.key from t3_n35 a left semi join t2_n87 b on a.key = b.key left outer join t1_n148 c on a.value = c.value sort by a.key;
select a.key from t3_n35 a left semi join t2_n87 b on a.key = b.key left outer join t1_n148 c on a.value = c.value sort by a.key;

explain vectorization operator
select a.key from t3_n35 a left semi join t2_n87 b on a.value = b.value where a.key > 100;
select a.key from t3_n35 a left semi join t2_n87 b on a.value = b.value where a.key > 100;

set hive.vectorized.execution.enabled=true;
set hive.mapjoin.hybridgrace.hashtable=true;
set hive.llap.enable.grace.join.in.llap=true;
SET hive.vectorized.execution.mapjoin.native.enabled=true;

explain vectorization operator
select * from t1_n148 a left semi join t2_n87 b on a.key=b.key sort by a.key, a.value;
select * from t1_n148 a left semi join t2_n87 b on a.key=b.key sort by a.key, a.value;

explain vectorization operator
select * from t2_n87 a left semi join t1_n148 b on b.key=a.key sort by a.key, a.value;
select * from t2_n87 a left semi join t1_n148 b on b.key=a.key sort by a.key, a.value;

explain vectorization operator
select * from t1_n148 a left semi join t4_n19 b on b.key=a.key sort by a.key, a.value;
select * from t1_n148 a left semi join t4_n19 b on b.key=a.key sort by a.key, a.value;

explain vectorization operator
select a.value from t1_n148 a left semi join t3_n35 b on (b.key = a.key and b.key < '15') sort by a.value;
select a.value from t1_n148 a left semi join t3_n35 b on (b.key = a.key and b.key < '15') sort by a.value;

explain vectorization operator
select * from t1_n148 a left semi join t2_n87 b on a.key = b.key and b.value < "val_10" sort by a.key, a.value;
select * from t1_n148 a left semi join t2_n87 b on a.key = b.key and b.value < "val_10" sort by a.key, a.value;

explain vectorization operator
select a.value from t1_n148 a left semi join (select key from t3_n35 where key > 5) b on a.key = b.key sort by a.value;
select a.value from t1_n148 a left semi join (select key from t3_n35 where key > 5) b on a.key = b.key sort by a.value;

explain vectorization operator
select a.value from t1_n148 a left semi join (select key , value from t2_n87 where key > 5) b on a.key = b.key and b.value <= 'val_20' sort by a.value ;
select a.value from t1_n148 a left semi join (select key , value from t2_n87 where key > 5) b on a.key = b.key and b.value <= 'val_20' sort by a.value ;

explain vectorization operator
select * from t2_n87 a left semi join (select key , value from t1_n148 where key > 2) b on a.key = b.key sort by a.key, a.value;
select * from t2_n87 a left semi join (select key , value from t1_n148 where key > 2) b on a.key = b.key sort by a.key, a.value;

explain vectorization operator
select /*+ mapjoin(b) */ a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key sort by a.key;
select /*+ mapjoin(b) */ a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key sort by a.key;

explain vectorization operator
select * from t1_n148 a left semi join t2_n87 b on a.key = 2*b.key sort by a.key, a.value;
select * from t1_n148 a left semi join t2_n87 b on a.key = 2*b.key sort by a.key, a.value;

explain vectorization operator
select * from t1_n148 a join t2_n87 b on a.key = b.key left semi join t3_n35 c on b.key = c.key sort by a.key, a.value;
select * from t1_n148 a join t2_n87 b on a.key = b.key left semi join t3_n35 c on b.key = c.key sort by a.key, a.value;
 
explain vectorization operator
select * from t3_n35 a left semi join t1_n148 b on a.key = b.key and a.value=b.value sort by a.key, a.value;
select * from t3_n35 a left semi join t1_n148 b on a.key = b.key and a.value=b.value sort by a.key, a.value;

explain vectorization operator
select /*+ mapjoin(b, c) */ a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key left semi join t2_n87 c on a.key = c.key sort by a.key;
select /*+ mapjoin(b, c) */ a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key left semi join t2_n87 c on a.key = c.key sort by a.key;

explain vectorization operator
select a.key from t3_n35 a left outer join t1_n148 b on a.key = b.key left semi join t2_n87 c on b.key = c.key sort by a.key;
select a.key from t3_n35 a left outer join t1_n148 b on a.key = b.key left semi join t2_n87 c on b.key = c.key sort by a.key;

SET hive.mapjoin.full.outer=false;
explain vectorization operator
select a.key from t1_n148 a full outer join t3_n35 b on a.key = b.key left semi join t2_n87 c on b.key = c.key sort by a.key;
select a.key from t1_n148 a full outer join t3_n35 b on a.key = b.key left semi join t2_n87 c on b.key = c.key sort by a.key;

SET hive.mapjoin.full.outer=true;
explain vectorization operator
select a.key from t1_n148 a full outer join t3_n35 b on a.key = b.key left semi join t2_n87 c on b.key = c.key sort by a.key;
select a.key from t1_n148 a full outer join t3_n35 b on a.key = b.key left semi join t2_n87 c on b.key = c.key sort by a.key;

explain vectorization operator
select a.key from t3_n35 a left semi join t2_n87 b on a.key = b.key left outer join t1_n148 c on a.key = c.key sort by a.key;
select a.key from t3_n35 a left semi join t2_n87 b on a.key = b.key left outer join t1_n148 c on a.key = c.key sort by a.key;

SET hive.mapjoin.full.outer=false;
explain vectorization operator
select a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key full outer join t2_n87 c on a.key = c.key sort by a.key;
select a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key full outer join t2_n87 c on a.key = c.key sort by a.key;

SET hive.mapjoin.full.outer=true;
explain vectorization operator
select a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key full outer join t2_n87 c on a.key = c.key sort by a.key;
select a.key from t3_n35 a left semi join t1_n148 b on a.key = b.key full outer join t2_n87 c on a.key = c.key sort by a.key;

explain vectorization operator
select a.key from t3_n35 a left semi join t2_n87 b on a.key = b.key left outer join t1_n148 c on a.value = c.value sort by a.key;
select a.key from t3_n35 a left semi join t2_n87 b on a.key = b.key left outer join t1_n148 c on a.value = c.value sort by a.key;

explain vectorization operator
select a.key from t3_n35 a left semi join t2_n87 b on a.value = b.value where a.key > 100;
select a.key from t3_n35 a left semi join t2_n87 b on a.value = b.value where a.key > 100;
