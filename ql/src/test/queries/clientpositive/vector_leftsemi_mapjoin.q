set hive.explain.user=false;
set hive.fetch.task.conversion=none;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000;

-- SORT_QUERY_RESULTS

create table t1 stored as orc as select cast(key as int) key, value from src where key <= 10;

select * from t1 sort by key;

create table t2 stored as orc as select cast(2*key as int) key, value from t1;

select * from t2 sort by key;

create table t3 stored as orc as select * from (select * from t1 union all select * from t2) b;
select * from t3 sort by key, value;

analyze table t3 compute statistics;

create table t4 (key int, value string) stored as orc;
select * from t4;


set hive.vectorized.execution.enabled=false;
set hive.mapjoin.hybridgrace.hashtable=false;

explain vectorization only summary

select * from t1 a left semi join t2 b on a.key=b.key sort by a.key, a.value;
select * from t1 a left semi join t2 b on a.key=b.key sort by a.key, a.value;

explain vectorization only summary
select * from t2 a left semi join t1 b on b.key=a.key sort by a.key, a.value;
select * from t2 a left semi join t1 b on b.key=a.key sort by a.key, a.value;

explain vectorization only summary
select * from t1 a left semi join t4 b on b.key=a.key sort by a.key, a.value;
select * from t1 a left semi join t4 b on b.key=a.key sort by a.key, a.value;

explain vectorization only summary
select a.value from t1 a left semi join t3 b on (b.key = a.key and b.key < '15') sort by a.value;
select a.value from t1 a left semi join t3 b on (b.key = a.key and b.key < '15') sort by a.value;

explain vectorization only summary
select * from t1 a left semi join t2 b on a.key = b.key and b.value < "val_10" sort by a.key, a.value;
select * from t1 a left semi join t2 b on a.key = b.key and b.value < "val_10" sort by a.key, a.value;

explain vectorization only summary
select a.value from t1 a left semi join (select key from t3 where key > 5) b on a.key = b.key sort by a.value;
select a.value from t1 a left semi join (select key from t3 where key > 5) b on a.key = b.key sort by a.value;

explain vectorization only summary
select a.value from t1 a left semi join (select key , value from t2 where key > 5) b on a.key = b.key and b.value <= 'val_20' sort by a.value ;
select a.value from t1 a left semi join (select key , value from t2 where key > 5) b on a.key = b.key and b.value <= 'val_20' sort by a.value ;

explain vectorization only summary
select * from t2 a left semi join (select key , value from t1 where key > 2) b on a.key = b.key sort by a.key, a.value;
select * from t2 a left semi join (select key , value from t1 where key > 2) b on a.key = b.key sort by a.key, a.value;

explain vectorization only summary
select /*+ mapjoin(b) */ a.key from t3 a left semi join t1 b on a.key = b.key sort by a.key;
select /*+ mapjoin(b) */ a.key from t3 a left semi join t1 b on a.key = b.key sort by a.key;

explain vectorization only summary
select * from t1 a left semi join t2 b on a.key = 2*b.key sort by a.key, a.value;
select * from t1 a left semi join t2 b on a.key = 2*b.key sort by a.key, a.value;

explain vectorization only summary
select * from t1 a join t2 b on a.key = b.key left semi join t3 c on b.key = c.key sort by a.key, a.value;
select * from t1 a join t2 b on a.key = b.key left semi join t3 c on b.key = c.key sort by a.key, a.value;
 
explain vectorization only summary
select * from t3 a left semi join t1 b on a.key = b.key and a.value=b.value sort by a.key, a.value;
select * from t3 a left semi join t1 b on a.key = b.key and a.value=b.value sort by a.key, a.value;

explain vectorization only summary
select /*+ mapjoin(b, c) */ a.key from t3 a left semi join t1 b on a.key = b.key left semi join t2 c on a.key = c.key sort by a.key;
select /*+ mapjoin(b, c) */ a.key from t3 a left semi join t1 b on a.key = b.key left semi join t2 c on a.key = c.key sort by a.key;

explain vectorization only summary
select a.key from t3 a left outer join t1 b on a.key = b.key left semi join t2 c on b.key = c.key sort by a.key;
select a.key from t3 a left outer join t1 b on a.key = b.key left semi join t2 c on b.key = c.key sort by a.key;

explain vectorization only summary
select a.key from t1 a full outer join t3 b on a.key = b.key left semi join t2 c on b.key = c.key sort by a.key;
select a.key from t1 a full outer join t3 b on a.key = b.key left semi join t2 c on b.key = c.key sort by a.key;

explain vectorization only summary
select a.key from t3 a left semi join t2 b on a.key = b.key left outer join t1 c on a.key = c.key sort by a.key;
select a.key from t3 a left semi join t2 b on a.key = b.key left outer join t1 c on a.key = c.key sort by a.key;

explain vectorization only summary
select a.key from t3 a left semi join t2 b on a.key = b.key right outer join t1 c on a.key = c.key sort by a.key;
select a.key from t3 a left semi join t2 b on a.key = b.key right outer join t1 c on a.key = c.key sort by a.key;

explain vectorization only summary
select a.key from t3 a left semi join t1 b on a.key = b.key full outer join t2 c on a.key = c.key sort by a.key;
select a.key from t3 a left semi join t1 b on a.key = b.key full outer join t2 c on a.key = c.key sort by a.key;

explain vectorization only summary
select a.key from t3 a left semi join t2 b on a.key = b.key left outer join t1 c on a.value = c.value sort by a.key;
select a.key from t3 a left semi join t2 b on a.key = b.key left outer join t1 c on a.value = c.value sort by a.key;

explain vectorization only summary
select a.key from t3 a left semi join t2 b on a.value = b.value where a.key > 100;
select a.key from t3 a left semi join t2 b on a.value = b.value where a.key > 100;

set hive.vectorized.execution.enabled=false;
set hive.mapjoin.hybridgrace.hashtable=true;

explain vectorization summary
select * from t1 a left semi join t2 b on a.key=b.key sort by a.key, a.value;
select * from t1 a left semi join t2 b on a.key=b.key sort by a.key, a.value;

explain vectorization summary
select * from t2 a left semi join t1 b on b.key=a.key sort by a.key, a.value;
select * from t2 a left semi join t1 b on b.key=a.key sort by a.key, a.value;

explain vectorization summary
select * from t1 a left semi join t4 b on b.key=a.key sort by a.key, a.value;
select * from t1 a left semi join t4 b on b.key=a.key sort by a.key, a.value;

explain vectorization summary
select a.value from t1 a left semi join t3 b on (b.key = a.key and b.key < '15') sort by a.value;
select a.value from t1 a left semi join t3 b on (b.key = a.key and b.key < '15') sort by a.value;

explain vectorization summary
select * from t1 a left semi join t2 b on a.key = b.key and b.value < "val_10" sort by a.key, a.value;
select * from t1 a left semi join t2 b on a.key = b.key and b.value < "val_10" sort by a.key, a.value;

explain vectorization summary
select a.value from t1 a left semi join (select key from t3 where key > 5) b on a.key = b.key sort by a.value;
select a.value from t1 a left semi join (select key from t3 where key > 5) b on a.key = b.key sort by a.value;

explain vectorization summary
select a.value from t1 a left semi join (select key , value from t2 where key > 5) b on a.key = b.key and b.value <= 'val_20' sort by a.value ;
select a.value from t1 a left semi join (select key , value from t2 where key > 5) b on a.key = b.key and b.value <= 'val_20' sort by a.value ;

explain vectorization summary
select * from t2 a left semi join (select key , value from t1 where key > 2) b on a.key = b.key sort by a.key, a.value;
select * from t2 a left semi join (select key , value from t1 where key > 2) b on a.key = b.key sort by a.key, a.value;

explain vectorization summary
select /*+ mapjoin(b) */ a.key from t3 a left semi join t1 b on a.key = b.key sort by a.key;
select /*+ mapjoin(b) */ a.key from t3 a left semi join t1 b on a.key = b.key sort by a.key;

explain vectorization summary
select * from t1 a left semi join t2 b on a.key = 2*b.key sort by a.key, a.value;
select * from t1 a left semi join t2 b on a.key = 2*b.key sort by a.key, a.value;

explain vectorization summary
select * from t1 a join t2 b on a.key = b.key left semi join t3 c on b.key = c.key sort by a.key, a.value;
select * from t1 a join t2 b on a.key = b.key left semi join t3 c on b.key = c.key sort by a.key, a.value;
 
explain vectorization summary
select * from t3 a left semi join t1 b on a.key = b.key and a.value=b.value sort by a.key, a.value;
select * from t3 a left semi join t1 b on a.key = b.key and a.value=b.value sort by a.key, a.value;

explain vectorization summary
select /*+ mapjoin(b, c) */ a.key from t3 a left semi join t1 b on a.key = b.key left semi join t2 c on a.key = c.key sort by a.key;
select /*+ mapjoin(b, c) */ a.key from t3 a left semi join t1 b on a.key = b.key left semi join t2 c on a.key = c.key sort by a.key;

explain vectorization summary
select a.key from t3 a left outer join t1 b on a.key = b.key left semi join t2 c on b.key = c.key sort by a.key;
select a.key from t3 a left outer join t1 b on a.key = b.key left semi join t2 c on b.key = c.key sort by a.key;

explain vectorization summary
select a.key from t1 a full outer join t3 b on a.key = b.key left semi join t2 c on b.key = c.key sort by a.key;
select a.key from t1 a full outer join t3 b on a.key = b.key left semi join t2 c on b.key = c.key sort by a.key;

explain vectorization summary
select a.key from t3 a left semi join t2 b on a.key = b.key left outer join t1 c on a.key = c.key sort by a.key;
select a.key from t3 a left semi join t2 b on a.key = b.key left outer join t1 c on a.key = c.key sort by a.key;

explain vectorization summary
select a.key from t3 a left semi join t2 b on a.key = b.key right outer join t1 c on a.key = c.key sort by a.key;
select a.key from t3 a left semi join t2 b on a.key = b.key right outer join t1 c on a.key = c.key sort by a.key;

explain vectorization summary
select a.key from t3 a left semi join t1 b on a.key = b.key full outer join t2 c on a.key = c.key sort by a.key;
select a.key from t3 a left semi join t1 b on a.key = b.key full outer join t2 c on a.key = c.key sort by a.key;

explain vectorization summary
select a.key from t3 a left semi join t2 b on a.key = b.key left outer join t1 c on a.value = c.value sort by a.key;
select a.key from t3 a left semi join t2 b on a.key = b.key left outer join t1 c on a.value = c.value sort by a.key;

explain vectorization summary
select a.key from t3 a left semi join t2 b on a.value = b.value where a.key > 100;
select a.key from t3 a left semi join t2 b on a.value = b.value where a.key > 100;

set hive.vectorized.execution.enabled=true;
set hive.mapjoin.hybridgrace.hashtable=false;
SET hive.vectorized.execution.mapjoin.native.enabled=false;

explain vectorization only operator
select * from t1 a left semi join t2 b on a.key=b.key sort by a.key, a.value;
select * from t1 a left semi join t2 b on a.key=b.key sort by a.key, a.value;

explain vectorization only operator
select * from t2 a left semi join t1 b on b.key=a.key sort by a.key, a.value;
select * from t2 a left semi join t1 b on b.key=a.key sort by a.key, a.value;

explain vectorization only operator
select * from t1 a left semi join t4 b on b.key=a.key sort by a.key, a.value;
select * from t1 a left semi join t4 b on b.key=a.key sort by a.key, a.value;

explain vectorization only operator
select a.value from t1 a left semi join t3 b on (b.key = a.key and b.key < '15') sort by a.value;
select a.value from t1 a left semi join t3 b on (b.key = a.key and b.key < '15') sort by a.value;

explain vectorization only operator
select * from t1 a left semi join t2 b on a.key = b.key and b.value < "val_10" sort by a.key, a.value;
select * from t1 a left semi join t2 b on a.key = b.key and b.value < "val_10" sort by a.key, a.value;

explain vectorization only operator
select a.value from t1 a left semi join (select key from t3 where key > 5) b on a.key = b.key sort by a.value;
select a.value from t1 a left semi join (select key from t3 where key > 5) b on a.key = b.key sort by a.value;

explain vectorization only operator
select a.value from t1 a left semi join (select key , value from t2 where key > 5) b on a.key = b.key and b.value <= 'val_20' sort by a.value ;
select a.value from t1 a left semi join (select key , value from t2 where key > 5) b on a.key = b.key and b.value <= 'val_20' sort by a.value ;

explain vectorization only operator
select * from t2 a left semi join (select key , value from t1 where key > 2) b on a.key = b.key sort by a.key, a.value;
select * from t2 a left semi join (select key , value from t1 where key > 2) b on a.key = b.key sort by a.key, a.value;

explain vectorization only operator
select /*+ mapjoin(b) */ a.key from t3 a left semi join t1 b on a.key = b.key sort by a.key;
select /*+ mapjoin(b) */ a.key from t3 a left semi join t1 b on a.key = b.key sort by a.key;

explain vectorization only operator
select * from t1 a left semi join t2 b on a.key = 2*b.key sort by a.key, a.value;
select * from t1 a left semi join t2 b on a.key = 2*b.key sort by a.key, a.value;

explain vectorization only operator
select * from t1 a join t2 b on a.key = b.key left semi join t3 c on b.key = c.key sort by a.key, a.value;
select * from t1 a join t2 b on a.key = b.key left semi join t3 c on b.key = c.key sort by a.key, a.value;
 
explain vectorization only operator
select * from t3 a left semi join t1 b on a.key = b.key and a.value=b.value sort by a.key, a.value;
select * from t3 a left semi join t1 b on a.key = b.key and a.value=b.value sort by a.key, a.value;

explain vectorization only operator
select /*+ mapjoin(b, c) */ a.key from t3 a left semi join t1 b on a.key = b.key left semi join t2 c on a.key = c.key sort by a.key;
select /*+ mapjoin(b, c) */ a.key from t3 a left semi join t1 b on a.key = b.key left semi join t2 c on a.key = c.key sort by a.key;

explain vectorization only operator
select a.key from t3 a left outer join t1 b on a.key = b.key left semi join t2 c on b.key = c.key sort by a.key;
select a.key from t3 a left outer join t1 b on a.key = b.key left semi join t2 c on b.key = c.key sort by a.key;

explain vectorization only operator
select a.key from t1 a full outer join t3 b on a.key = b.key left semi join t2 c on b.key = c.key sort by a.key;
select a.key from t1 a full outer join t3 b on a.key = b.key left semi join t2 c on b.key = c.key sort by a.key;

explain vectorization only operator
select a.key from t3 a left semi join t2 b on a.key = b.key left outer join t1 c on a.key = c.key sort by a.key;
select a.key from t3 a left semi join t2 b on a.key = b.key left outer join t1 c on a.key = c.key sort by a.key;

explain vectorization only operator
select a.key from t3 a left semi join t2 b on a.key = b.key right outer join t1 c on a.key = c.key sort by a.key;
select a.key from t3 a left semi join t2 b on a.key = b.key right outer join t1 c on a.key = c.key sort by a.key;

explain vectorization only operator
select a.key from t3 a left semi join t1 b on a.key = b.key full outer join t2 c on a.key = c.key sort by a.key;
select a.key from t3 a left semi join t1 b on a.key = b.key full outer join t2 c on a.key = c.key sort by a.key;

explain vectorization only operator
select a.key from t3 a left semi join t2 b on a.key = b.key left outer join t1 c on a.value = c.value sort by a.key;
select a.key from t3 a left semi join t2 b on a.key = b.key left outer join t1 c on a.value = c.value sort by a.key;

explain vectorization only operator
select a.key from t3 a left semi join t2 b on a.value = b.value where a.key > 100;
select a.key from t3 a left semi join t2 b on a.value = b.value where a.key > 100;

set hive.vectorized.execution.enabled=true;
set hive.mapjoin.hybridgrace.hashtable=true;
SET hive.vectorized.execution.mapjoin.native.enabled=false;

explain vectorization detail
select * from t1 a left semi join t2 b on a.key=b.key sort by a.key, a.value;
select * from t1 a left semi join t2 b on a.key=b.key sort by a.key, a.value;

explain vectorization detail
select * from t2 a left semi join t1 b on b.key=a.key sort by a.key, a.value;
select * from t2 a left semi join t1 b on b.key=a.key sort by a.key, a.value;

explain vectorization detail
select * from t1 a left semi join t4 b on b.key=a.key sort by a.key, a.value;
select * from t1 a left semi join t4 b on b.key=a.key sort by a.key, a.value;

explain vectorization detail
select a.value from t1 a left semi join t3 b on (b.key = a.key and b.key < '15') sort by a.value;
select a.value from t1 a left semi join t3 b on (b.key = a.key and b.key < '15') sort by a.value;

explain vectorization detail
select * from t1 a left semi join t2 b on a.key = b.key and b.value < "val_10" sort by a.key, a.value;
select * from t1 a left semi join t2 b on a.key = b.key and b.value < "val_10" sort by a.key, a.value;

explain vectorization detail
select a.value from t1 a left semi join (select key from t3 where key > 5) b on a.key = b.key sort by a.value;
select a.value from t1 a left semi join (select key from t3 where key > 5) b on a.key = b.key sort by a.value;

explain vectorization detail
select a.value from t1 a left semi join (select key , value from t2 where key > 5) b on a.key = b.key and b.value <= 'val_20' sort by a.value ;
select a.value from t1 a left semi join (select key , value from t2 where key > 5) b on a.key = b.key and b.value <= 'val_20' sort by a.value ;

explain vectorization detail
select * from t2 a left semi join (select key , value from t1 where key > 2) b on a.key = b.key sort by a.key, a.value;
select * from t2 a left semi join (select key , value from t1 where key > 2) b on a.key = b.key sort by a.key, a.value;

explain vectorization detail
select /*+ mapjoin(b) */ a.key from t3 a left semi join t1 b on a.key = b.key sort by a.key;
select /*+ mapjoin(b) */ a.key from t3 a left semi join t1 b on a.key = b.key sort by a.key;

explain vectorization detail
select * from t1 a left semi join t2 b on a.key = 2*b.key sort by a.key, a.value;
select * from t1 a left semi join t2 b on a.key = 2*b.key sort by a.key, a.value;

explain vectorization detail
select * from t1 a join t2 b on a.key = b.key left semi join t3 c on b.key = c.key sort by a.key, a.value;
select * from t1 a join t2 b on a.key = b.key left semi join t3 c on b.key = c.key sort by a.key, a.value;
 
explain vectorization detail
select * from t3 a left semi join t1 b on a.key = b.key and a.value=b.value sort by a.key, a.value;
select * from t3 a left semi join t1 b on a.key = b.key and a.value=b.value sort by a.key, a.value;

explain vectorization detail
select /*+ mapjoin(b, c) */ a.key from t3 a left semi join t1 b on a.key = b.key left semi join t2 c on a.key = c.key sort by a.key;
select /*+ mapjoin(b, c) */ a.key from t3 a left semi join t1 b on a.key = b.key left semi join t2 c on a.key = c.key sort by a.key;

explain vectorization detail
select a.key from t3 a left outer join t1 b on a.key = b.key left semi join t2 c on b.key = c.key sort by a.key;
select a.key from t3 a left outer join t1 b on a.key = b.key left semi join t2 c on b.key = c.key sort by a.key;

explain vectorization detail
select a.key from t1 a full outer join t3 b on a.key = b.key left semi join t2 c on b.key = c.key sort by a.key;
select a.key from t1 a full outer join t3 b on a.key = b.key left semi join t2 c on b.key = c.key sort by a.key;

explain vectorization detail
select a.key from t3 a left semi join t2 b on a.key = b.key left outer join t1 c on a.key = c.key sort by a.key;
select a.key from t3 a left semi join t2 b on a.key = b.key left outer join t1 c on a.key = c.key sort by a.key;

explain vectorization detail
select a.key from t3 a left semi join t2 b on a.key = b.key right outer join t1 c on a.key = c.key sort by a.key;
select a.key from t3 a left semi join t2 b on a.key = b.key right outer join t1 c on a.key = c.key sort by a.key;

explain vectorization detail
select a.key from t3 a left semi join t1 b on a.key = b.key full outer join t2 c on a.key = c.key sort by a.key;
select a.key from t3 a left semi join t1 b on a.key = b.key full outer join t2 c on a.key = c.key sort by a.key;

explain vectorization detail
select a.key from t3 a left semi join t2 b on a.key = b.key left outer join t1 c on a.value = c.value sort by a.key;
select a.key from t3 a left semi join t2 b on a.key = b.key left outer join t1 c on a.value = c.value sort by a.key;

explain vectorization detail
select a.key from t3 a left semi join t2 b on a.value = b.value where a.key > 100;
select a.key from t3 a left semi join t2 b on a.value = b.value where a.key > 100;

set hive.vectorized.execution.enabled=true;
set hive.mapjoin.hybridgrace.hashtable=false;
SET hive.vectorized.execution.mapjoin.native.enabled=true;

explain vectorization detail
select * from t1 a left semi join t2 b on a.key=b.key sort by a.key, a.value;
select * from t1 a left semi join t2 b on a.key=b.key sort by a.key, a.value;

explain vectorization detail
select * from t2 a left semi join t1 b on b.key=a.key sort by a.key, a.value;
select * from t2 a left semi join t1 b on b.key=a.key sort by a.key, a.value;

explain vectorization detail
select * from t1 a left semi join t4 b on b.key=a.key sort by a.key, a.value;
select * from t1 a left semi join t4 b on b.key=a.key sort by a.key, a.value;

explain vectorization detail
select a.value from t1 a left semi join t3 b on (b.key = a.key and b.key < '15') sort by a.value;
select a.value from t1 a left semi join t3 b on (b.key = a.key and b.key < '15') sort by a.value;

explain vectorization detail
select * from t1 a left semi join t2 b on a.key = b.key and b.value < "val_10" sort by a.key, a.value;
select * from t1 a left semi join t2 b on a.key = b.key and b.value < "val_10" sort by a.key, a.value;

explain vectorization detail
select a.value from t1 a left semi join (select key from t3 where key > 5) b on a.key = b.key sort by a.value;
select a.value from t1 a left semi join (select key from t3 where key > 5) b on a.key = b.key sort by a.value;

explain vectorization detail
select a.value from t1 a left semi join (select key , value from t2 where key > 5) b on a.key = b.key and b.value <= 'val_20' sort by a.value ;
select a.value from t1 a left semi join (select key , value from t2 where key > 5) b on a.key = b.key and b.value <= 'val_20' sort by a.value ;

explain vectorization detail
select * from t2 a left semi join (select key , value from t1 where key > 2) b on a.key = b.key sort by a.key, a.value;
select * from t2 a left semi join (select key , value from t1 where key > 2) b on a.key = b.key sort by a.key, a.value;

explain vectorization detail
select /*+ mapjoin(b) */ a.key from t3 a left semi join t1 b on a.key = b.key sort by a.key;
select /*+ mapjoin(b) */ a.key from t3 a left semi join t1 b on a.key = b.key sort by a.key;

explain vectorization detail
select * from t1 a left semi join t2 b on a.key = 2*b.key sort by a.key, a.value;
select * from t1 a left semi join t2 b on a.key = 2*b.key sort by a.key, a.value;

explain vectorization detail
select * from t1 a join t2 b on a.key = b.key left semi join t3 c on b.key = c.key sort by a.key, a.value;
select * from t1 a join t2 b on a.key = b.key left semi join t3 c on b.key = c.key sort by a.key, a.value;
 
explain vectorization detail
select * from t3 a left semi join t1 b on a.key = b.key and a.value=b.value sort by a.key, a.value;
select * from t3 a left semi join t1 b on a.key = b.key and a.value=b.value sort by a.key, a.value;

explain vectorization detail
select /*+ mapjoin(b, c) */ a.key from t3 a left semi join t1 b on a.key = b.key left semi join t2 c on a.key = c.key sort by a.key;
select /*+ mapjoin(b, c) */ a.key from t3 a left semi join t1 b on a.key = b.key left semi join t2 c on a.key = c.key sort by a.key;

explain vectorization detail
select a.key from t3 a left outer join t1 b on a.key = b.key left semi join t2 c on b.key = c.key sort by a.key;
select a.key from t3 a left outer join t1 b on a.key = b.key left semi join t2 c on b.key = c.key sort by a.key;

explain vectorization detail
select a.key from t1 a full outer join t3 b on a.key = b.key left semi join t2 c on b.key = c.key sort by a.key;
select a.key from t1 a full outer join t3 b on a.key = b.key left semi join t2 c on b.key = c.key sort by a.key;

explain vectorization detail
select a.key from t3 a left semi join t2 b on a.key = b.key left outer join t1 c on a.key = c.key sort by a.key;
select a.key from t3 a left semi join t2 b on a.key = b.key left outer join t1 c on a.key = c.key sort by a.key;

explain vectorization detail
select a.key from t3 a left semi join t2 b on a.key = b.key right outer join t1 c on a.key = c.key sort by a.key;
select a.key from t3 a left semi join t2 b on a.key = b.key right outer join t1 c on a.key = c.key sort by a.key;

explain vectorization detail
select a.key from t3 a left semi join t1 b on a.key = b.key full outer join t2 c on a.key = c.key sort by a.key;
select a.key from t3 a left semi join t1 b on a.key = b.key full outer join t2 c on a.key = c.key sort by a.key;

explain vectorization detail
select a.key from t3 a left semi join t2 b on a.key = b.key left outer join t1 c on a.value = c.value sort by a.key;
select a.key from t3 a left semi join t2 b on a.key = b.key left outer join t1 c on a.value = c.value sort by a.key;

explain vectorization detail
select a.key from t3 a left semi join t2 b on a.value = b.value where a.key > 100;
select a.key from t3 a left semi join t2 b on a.value = b.value where a.key > 100;

set hive.vectorized.execution.enabled=true;
set hive.mapjoin.hybridgrace.hashtable=true;
SET hive.vectorized.execution.mapjoin.native.enabled=true;

explain vectorization detail
select * from t1 a left semi join t2 b on a.key=b.key sort by a.key, a.value;
select * from t1 a left semi join t2 b on a.key=b.key sort by a.key, a.value;

explain vectorization detail
select * from t2 a left semi join t1 b on b.key=a.key sort by a.key, a.value;
select * from t2 a left semi join t1 b on b.key=a.key sort by a.key, a.value;

explain vectorization detail
select * from t1 a left semi join t4 b on b.key=a.key sort by a.key, a.value;
select * from t1 a left semi join t4 b on b.key=a.key sort by a.key, a.value;

explain vectorization detail
select a.value from t1 a left semi join t3 b on (b.key = a.key and b.key < '15') sort by a.value;
select a.value from t1 a left semi join t3 b on (b.key = a.key and b.key < '15') sort by a.value;

explain vectorization detail
select * from t1 a left semi join t2 b on a.key = b.key and b.value < "val_10" sort by a.key, a.value;
select * from t1 a left semi join t2 b on a.key = b.key and b.value < "val_10" sort by a.key, a.value;

explain vectorization detail
select a.value from t1 a left semi join (select key from t3 where key > 5) b on a.key = b.key sort by a.value;
select a.value from t1 a left semi join (select key from t3 where key > 5) b on a.key = b.key sort by a.value;

explain vectorization detail
select a.value from t1 a left semi join (select key , value from t2 where key > 5) b on a.key = b.key and b.value <= 'val_20' sort by a.value ;
select a.value from t1 a left semi join (select key , value from t2 where key > 5) b on a.key = b.key and b.value <= 'val_20' sort by a.value ;

explain vectorization detail
select * from t2 a left semi join (select key , value from t1 where key > 2) b on a.key = b.key sort by a.key, a.value;
select * from t2 a left semi join (select key , value from t1 where key > 2) b on a.key = b.key sort by a.key, a.value;

explain vectorization detail
select /*+ mapjoin(b) */ a.key from t3 a left semi join t1 b on a.key = b.key sort by a.key;
select /*+ mapjoin(b) */ a.key from t3 a left semi join t1 b on a.key = b.key sort by a.key;

explain vectorization detail
select * from t1 a left semi join t2 b on a.key = 2*b.key sort by a.key, a.value;
select * from t1 a left semi join t2 b on a.key = 2*b.key sort by a.key, a.value;

explain vectorization detail
select * from t1 a join t2 b on a.key = b.key left semi join t3 c on b.key = c.key sort by a.key, a.value;
select * from t1 a join t2 b on a.key = b.key left semi join t3 c on b.key = c.key sort by a.key, a.value;
 
explain vectorization detail
select * from t3 a left semi join t1 b on a.key = b.key and a.value=b.value sort by a.key, a.value;
select * from t3 a left semi join t1 b on a.key = b.key and a.value=b.value sort by a.key, a.value;

explain vectorization detail
select /*+ mapjoin(b, c) */ a.key from t3 a left semi join t1 b on a.key = b.key left semi join t2 c on a.key = c.key sort by a.key;
select /*+ mapjoin(b, c) */ a.key from t3 a left semi join t1 b on a.key = b.key left semi join t2 c on a.key = c.key sort by a.key;

explain vectorization detail
select a.key from t3 a left outer join t1 b on a.key = b.key left semi join t2 c on b.key = c.key sort by a.key;
select a.key from t3 a left outer join t1 b on a.key = b.key left semi join t2 c on b.key = c.key sort by a.key;

explain vectorization detail
select a.key from t1 a full outer join t3 b on a.key = b.key left semi join t2 c on b.key = c.key sort by a.key;
select a.key from t1 a full outer join t3 b on a.key = b.key left semi join t2 c on b.key = c.key sort by a.key;

explain vectorization detail
select a.key from t3 a left semi join t2 b on a.key = b.key left outer join t1 c on a.key = c.key sort by a.key;
select a.key from t3 a left semi join t2 b on a.key = b.key left outer join t1 c on a.key = c.key sort by a.key;

explain vectorization detail
select a.key from t3 a left semi join t2 b on a.key = b.key right outer join t1 c on a.key = c.key sort by a.key;
select a.key from t3 a left semi join t2 b on a.key = b.key right outer join t1 c on a.key = c.key sort by a.key;

explain vectorization detail
select a.key from t3 a left semi join t1 b on a.key = b.key full outer join t2 c on a.key = c.key sort by a.key;
select a.key from t3 a left semi join t1 b on a.key = b.key full outer join t2 c on a.key = c.key sort by a.key;

explain vectorization detail
select a.key from t3 a left semi join t2 b on a.key = b.key left outer join t1 c on a.value = c.value sort by a.key;
select a.key from t3 a left semi join t2 b on a.key = b.key left outer join t1 c on a.value = c.value sort by a.key;

explain vectorization detail
select a.key from t3 a left semi join t2 b on a.value = b.value where a.key > 100;
select a.key from t3 a left semi join t2 b on a.value = b.value where a.key > 100;
