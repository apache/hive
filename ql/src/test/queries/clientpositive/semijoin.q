--! qt:dataset:src
SET hive.vectorized.execution.enabled=false;
set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS

create table t1_n55 as select cast(key as int) key, value from src where key <= 10;

select * from t1_n55 sort by key;

create table t2_n33 as select cast(2*key as int) key, value from t1_n55;

select * from t2_n33 sort by key;

create table t3_n12 as select * from (select * from t1_n55 union all select * from t2_n33) b;
select * from t3_n12 sort by key, value;

create table t4_n5 (key int, value string);
select * from t4_n5;

explain select * from t1_n55 a left semi join t2_n33 b on a.key=b.key sort by a.key, a.value;
select * from t1_n55 a left semi join t2_n33 b on a.key=b.key sort by a.key, a.value;

explain select * from t2_n33 a left semi join t1_n55 b on b.key=a.key sort by a.key, a.value;
select * from t2_n33 a left semi join t1_n55 b on b.key=a.key sort by a.key, a.value;

explain select * from t1_n55 a left semi join t4_n5 b on b.key=a.key sort by a.key, a.value;
select * from t1_n55 a left semi join t4_n5 b on b.key=a.key sort by a.key, a.value;

explain select a.value from t1_n55 a left semi join t3_n12 b on (b.key = a.key and b.key < '15') sort by a.value;
select a.value from t1_n55 a left semi join t3_n12 b on (b.key = a.key and b.key < '15') sort by a.value;

explain select * from t1_n55 a left semi join t2_n33 b on a.key = b.key and b.value < "val_10" sort by a.key, a.value;
select * from t1_n55 a left semi join t2_n33 b on a.key = b.key and b.value < "val_10" sort by a.key, a.value;

explain select a.value from t1_n55 a left semi join (select key from t3_n12 where key > 5) b on a.key = b.key sort by a.value;
select a.value from t1_n55 a left semi join (select key from t3_n12 where key > 5) b on a.key = b.key sort by a.value;

explain select a.value from t1_n55 a left semi join (select key , value from t2_n33 where key > 5) b on a.key = b.key and b.value <= 'val_20' sort by a.value ;
select a.value from t1_n55 a left semi join (select key , value from t2_n33 where key > 5) b on a.key = b.key and b.value <= 'val_20' sort by a.value ;

explain select * from t2_n33 a left semi join (select key , value from t1_n55 where key > 2) b on a.key = b.key sort by a.key, a.value;
select * from t2_n33 a left semi join (select key , value from t1_n55 where key > 2) b on a.key = b.key sort by a.key, a.value;

explain select /*+ mapjoin(b) */ a.key from t3_n12 a left semi join t1_n55 b on a.key = b.key sort by a.key;
select /*+ mapjoin(b) */ a.key from t3_n12 a left semi join t1_n55 b on a.key = b.key sort by a.key;

explain select * from t1_n55 a left semi join t2_n33 b on a.key = 2*b.key sort by a.key, a.value;
select * from t1_n55 a left semi join t2_n33 b on a.key = 2*b.key sort by a.key, a.value;

explain select * from t1_n55 a join t2_n33 b on a.key = b.key left semi join t3_n12 c on b.key = c.key sort by a.key, a.value;
select * from t1_n55 a join t2_n33 b on a.key = b.key left semi join t3_n12 c on b.key = c.key sort by a.key, a.value;
 
explain select * from t3_n12 a left semi join t1_n55 b on a.key = b.key and a.value=b.value sort by a.key, a.value;
select * from t3_n12 a left semi join t1_n55 b on a.key = b.key and a.value=b.value sort by a.key, a.value;

explain select /*+ mapjoin(b, c) */ a.key from t3_n12 a left semi join t1_n55 b on a.key = b.key left semi join t2_n33 c on a.key = c.key sort by a.key;
select /*+ mapjoin(b, c) */ a.key from t3_n12 a left semi join t1_n55 b on a.key = b.key left semi join t2_n33 c on a.key = c.key sort by a.key;

explain select a.key from t3_n12 a left outer join t1_n55 b on a.key = b.key left semi join t2_n33 c on b.key = c.key sort by a.key;
select a.key from t3_n12 a left outer join t1_n55 b on a.key = b.key left semi join t2_n33 c on b.key = c.key sort by a.key;

explain select a.key from t1_n55 a right outer join t3_n12 b on a.key = b.key left semi join t2_n33 c on b.key = c.key sort by a.key;
select a.key from t1_n55 a right outer join t3_n12 b on a.key = b.key left semi join t2_n33 c on b.key = c.key sort by a.key;

explain select a.key from t1_n55 a full outer join t3_n12 b on a.key = b.key left semi join t2_n33 c on b.key = c.key sort by a.key;
select a.key from t1_n55 a full outer join t3_n12 b on a.key = b.key left semi join t2_n33 c on b.key = c.key sort by a.key;

explain select a.key from t3_n12 a left semi join t2_n33 b on a.key = b.key left outer join t1_n55 c on a.key = c.key sort by a.key;
select a.key from t3_n12 a left semi join t2_n33 b on a.key = b.key left outer join t1_n55 c on a.key = c.key sort by a.key;

explain select a.key from t3_n12 a left semi join t2_n33 b on a.key = b.key right outer join t1_n55 c on a.key = c.key sort by a.key;
select a.key from t3_n12 a left semi join t2_n33 b on a.key = b.key right outer join t1_n55 c on a.key = c.key sort by a.key;

explain select a.key from t3_n12 a left semi join t1_n55 b on a.key = b.key full outer join t2_n33 c on a.key = c.key sort by a.key;
select a.key from t3_n12 a left semi join t1_n55 b on a.key = b.key full outer join t2_n33 c on a.key = c.key sort by a.key;

explain select a.key from t3_n12 a left semi join t2_n33 b on a.key = b.key left outer join t1_n55 c on a.value = c.value sort by a.key;
select a.key from t3_n12 a left semi join t2_n33 b on a.key = b.key left outer join t1_n55 c on a.value = c.value sort by a.key;

explain select a.key from t3_n12 a left semi join t2_n33 b on a.value = b.value where a.key > 100;
select a.key from t3_n12 a left semi join t2_n33 b on a.value = b.value where a.key > 100;

explain select key, value from src outr left semi join
    (select a.key, b.value from src a join (select distinct value from src) b on a.value > b.value group by a.key, b.value) inr
    on outr.key=inr.key and outr.value=inr.value;
select key, value from src outr left semi join
    (select a.key, b.value from src a join (select distinct value from src) b on a.value > b.value group by a.key, b.value) inr
    on outr.key=inr.key and outr.value=inr.value;
