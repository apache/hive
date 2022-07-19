set hive.cli.print.header=true;
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.ptf.enabled=true;
set hive.fetch.task.conversion=none;

CREATE TABLE vector_ptf_part_simple_text(p_mfgr string, p_name string, p_date date, p_retailprice double,
  p_char char(1), p_varchar varchar(5), p_boolean boolean, rowindex int)
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/vector_ptf_part_simple_all_datatypes.txt' OVERWRITE INTO TABLE vector_ptf_part_simple_text;

CREATE TABLE vector_ptf_part_simple_orc (p_mfgr string, p_name string, p_date date, p_timestamp timestamp,
  p_int int, p_retailprice double, p_decimal decimal(10,4), p_char char(1), p_varchar varchar(5),
  p_boolean boolean, rowindex int) stored as orc;

SELECT * FROM vector_ptf_part_simple_text;

INSERT INTO TABLE vector_ptf_part_simple_orc 
SELECT 
p_mfgr, p_name, p_date, 
CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(p_date)) as TIMESTAMP), 
CAST(UNIX_TIMESTAMP(p_date) as int), p_retailprice, 
CAST(p_retailprice as DECIMAL(10,4)),
p_char,
p_varchar,
p_boolean,
rowindex 
FROM vector_ptf_part_simple_text;

SELECT * FROM vector_ptf_part_simple_orc;

set hive.vectorized.execution.ptf.enabled=false;
select "************ NON_VECTORIZED REFERENCE ************";
select p_mfgr, p_name, rowindex, p_date, p_retailprice,
count(*) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as cs1,
count(*) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as cs2,
count(rowindex) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as c1,
count(rowindex) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as c2,
count(p_date) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as c_order,
sum(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as s1,
sum(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as s2,
min(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as min1,
min(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as min2,
max(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as max1,
max(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as max2,
avg(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as avg1,
avg(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as avg2,
row_number() over(partition by p_mfgr) as rn,
rank() over(partition by p_mfgr) as r,
dense_rank() over(partition by p_mfgr) as dr,
rank() over(partition by p_mfgr order by p_date) as r_date,
dense_rank() over(partition by p_mfgr order by p_date) as dr_date,
first_value(p_retailprice) over(partition by p_mfgr) as fv,
last_value(p_retailprice) over(partition by p_mfgr) as lv,
lead(p_retailprice) over(partition by p_mfgr) as lead1,
lag(p_retailprice) over(partition by p_mfgr) as lag1
from vector_ptf_part_simple_orc;


set hive.vectorized.execution.ptf.enabled=true;

EXPLAIN VECTORIZATION DETAIL
select p_mfgr, p_name, rowindex, p_date, p_retailprice,
count(*) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as cs1,
count(*) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as cs2,
count(rowindex) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as c1,
count(rowindex) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as c2,
count(p_date) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as c_order,
sum(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as s1,
sum(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as s2,
min(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as min1,
min(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as min2,
max(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as max1,
max(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as max2,
avg(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as avg1,
avg(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as avg2,
row_number() over(partition by p_mfgr) as rn,
rank() over(partition by p_mfgr) as r,
dense_rank() over(partition by p_mfgr) as dr,
rank() over(partition by p_mfgr order by p_date) as r_date,
dense_rank() over(partition by p_mfgr order by p_date) as dr_date,
first_value(p_retailprice) over(partition by p_mfgr) as fv,
last_value(p_retailprice) over(partition by p_mfgr) as lv,
lead(p_retailprice) over(partition by p_mfgr) as lead1,
lag(p_retailprice) over(partition by p_mfgr) as lag1
from vector_ptf_part_simple_orc;


select "************ BATCH SIZE=2, BUFFERED BATCHES: 2 ************";
set hive.vectorized.ptf.max.memory.buffering.batch.count=2;
set hive.vectorized.testing.reducer.batch.size=2;
select p_mfgr, p_name, rowindex, p_date, p_retailprice,
count(*) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as cs1,
count(*) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as cs2,
count(rowindex) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as c1,
count(rowindex) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as c2,
count(p_date) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as c_order,
sum(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as s1,
sum(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as s2,
min(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as min1,
min(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as min2,
max(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as max1,
max(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as max2,
avg(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as avg1,
avg(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as avg2,
row_number() over(partition by p_mfgr) as rn,
rank() over(partition by p_mfgr) as r,
dense_rank() over(partition by p_mfgr) as dr,
rank() over(partition by p_mfgr order by p_date) as r_date,
dense_rank() over(partition by p_mfgr order by p_date) as dr_date,
first_value(p_retailprice) over(partition by p_mfgr) as fv,
last_value(p_retailprice) over(partition by p_mfgr) as lv,
lead(p_retailprice) over(partition by p_mfgr) as lead1,
lag(p_retailprice) over(partition by p_mfgr) as lag1
from vector_ptf_part_simple_orc;


select "************ BATCH SIZE=2, BUFFERED BATCHES: 3 ************";
set hive.vectorized.ptf.max.memory.buffering.batch.count=3;
set hive.vectorized.testing.reducer.batch.size=2;
select p_mfgr, p_name, rowindex, p_date, p_retailprice,
count(*) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as cs1,
count(*) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as cs2,
count(rowindex) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as c1,
count(rowindex) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as c2,
count(p_date) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as c_order,
sum(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as s1,
sum(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as s2,
min(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as min1,
min(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as min2,
max(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as max1,
max(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as max2,
avg(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as avg1,
avg(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as avg2,
row_number() over(partition by p_mfgr) as rn,
rank() over(partition by p_mfgr) as r,
dense_rank() over(partition by p_mfgr) as dr,
rank() over(partition by p_mfgr order by p_date) as r_date,
dense_rank() over(partition by p_mfgr order by p_date) as dr_date,
first_value(p_retailprice) over(partition by p_mfgr) as fv,
last_value(p_retailprice) over(partition by p_mfgr) as lv,
lead(p_retailprice) over(partition by p_mfgr) as lead1,
lag(p_retailprice) over(partition by p_mfgr) as lag1
from vector_ptf_part_simple_orc;


select "************ BATCH SIZE=2, BUFFERED BATCHES: 4 ************";
set hive.vectorized.ptf.max.memory.buffering.batch.count=4;
set hive.vectorized.testing.reducer.batch.size=2;
select p_mfgr, p_name, rowindex, p_date, p_retailprice,
count(*) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as cs1,
count(*) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as cs2,
count(rowindex) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as c1,
count(rowindex) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as c2,
count(p_date) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as c_order,
sum(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as s1,
sum(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as s2,
min(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as min1,
min(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as min2,
max(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as max1,
max(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as max2,
avg(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as avg1,
avg(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as avg2,
row_number() over(partition by p_mfgr) as rn,
rank() over(partition by p_mfgr) as r,
dense_rank() over(partition by p_mfgr) as dr,
rank() over(partition by p_mfgr order by p_date) as r_date,
dense_rank() over(partition by p_mfgr order by p_date) as dr_date,
first_value(p_retailprice) over(partition by p_mfgr) as fv,
last_value(p_retailprice) over(partition by p_mfgr) as lv,
lead(p_retailprice) over(partition by p_mfgr) as lead1,
lag(p_retailprice) over(partition by p_mfgr) as lag1
from vector_ptf_part_simple_orc;


select "************ BATCH SIZE=1, BUFFERED BATCHES: 2 ************";
set hive.vectorized.ptf.max.memory.buffering.batch.count=2;
set hive.vectorized.testing.reducer.batch.size=1;
select p_mfgr, p_name, rowindex, p_date, p_retailprice,
count(*) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as cs1,
count(*) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as cs2,
count(rowindex) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as c1,
count(rowindex) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as c2,
count(p_date) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as c_order,
sum(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as s1,
sum(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as s2,
min(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as min1,
min(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as min2,
max(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as max1,
max(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as max2,
avg(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as avg1,
avg(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as avg2,
row_number() over(partition by p_mfgr) as rn,
rank() over(partition by p_mfgr) as r,
dense_rank() over(partition by p_mfgr) as dr,
rank() over(partition by p_mfgr order by p_date) as r_date,
dense_rank() over(partition by p_mfgr order by p_date) as dr_date,
first_value(p_retailprice) over(partition by p_mfgr) as fv,
last_value(p_retailprice) over(partition by p_mfgr) as lv,
lead(p_retailprice) over(partition by p_mfgr) as lead1,
lag(p_retailprice) over(partition by p_mfgr) as lag1
from vector_ptf_part_simple_orc;



select "************ BATCH SIZE=2, BUFFERED BATCHES: 3 ************";
set hive.vectorized.ptf.max.memory.buffering.batch.count=3;
set hive.vectorized.testing.reducer.batch.size=2;
select p_mfgr, p_name, rowindex, p_date, p_retailprice,
count(*) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as cs1,
count(*) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as cs2,
count(rowindex) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as c1,
count(rowindex) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as c2,
count(p_date) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as c_order,
sum(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as s1,
sum(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as s2,
min(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as min1,
min(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as min2,
max(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as max1,
max(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as max2,
avg(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as avg1,
avg(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as avg2,
row_number() over(partition by p_mfgr) as rn,
rank() over(partition by p_mfgr) as r,
dense_rank() over(partition by p_mfgr) as dr,
rank() over(partition by p_mfgr order by p_date) as r_date,
dense_rank() over(partition by p_mfgr order by p_date) as dr_date,
first_value(p_retailprice) over(partition by p_mfgr) as fv,
last_value(p_retailprice) over(partition by p_mfgr) as lv,
lead(p_retailprice) over(partition by p_mfgr) as lead1,
lag(p_retailprice) over(partition by p_mfgr) as lag1
from vector_ptf_part_simple_orc;


select "************ BATCH SIZE=2, BUFFERED BATCHES: 5 ************";
set hive.vectorized.ptf.max.memory.buffering.batch.count=5;
set hive.vectorized.testing.reducer.batch.size=2;
select p_mfgr, p_name, rowindex, p_date, p_retailprice,
count(*) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as cs1,
count(*) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as cs2,
count(rowindex) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as c1,
count(rowindex) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as c2,
count(p_date) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as c_order,
sum(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as s1,
sum(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as s2,
min(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as min1,
min(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as min2,
max(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as max1,
max(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as max2,
avg(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as avg1,
avg(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as avg2,
row_number() over(partition by p_mfgr) as rn,
rank() over(partition by p_mfgr) as r,
dense_rank() over(partition by p_mfgr) as dr,
rank() over(partition by p_mfgr order by p_date) as r_date,
dense_rank() over(partition by p_mfgr order by p_date) as dr_date,
first_value(p_retailprice) over(partition by p_mfgr) as fv,
last_value(p_retailprice) over(partition by p_mfgr) as lv,
lead(p_retailprice) over(partition by p_mfgr) as lead1,
lag(p_retailprice) over(partition by p_mfgr) as lag1
from vector_ptf_part_simple_orc;



select "************ BATCH SIZE=2, BUFFERED BATCHES: 10 ************";
set hive.vectorized.ptf.max.memory.buffering.batch.count=10;
set hive.vectorized.testing.reducer.batch.size=2;
select p_mfgr, p_name, rowindex, p_date, p_retailprice,
count(*) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as cs1,
count(*) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as cs2,
count(rowindex) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as c1,
count(rowindex) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as c2,
count(p_date) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as c_order,
sum(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as s1,
sum(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as s2,
min(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as min1,
min(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as min2,
max(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as max1,
max(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as max2,
avg(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as avg1,
avg(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as avg2,
row_number() over(partition by p_mfgr) as rn,
rank() over(partition by p_mfgr) as r,
dense_rank() over(partition by p_mfgr) as dr,
rank() over(partition by p_mfgr order by p_date) as r_date,
dense_rank() over(partition by p_mfgr order by p_date) as dr_date,
first_value(p_retailprice) over(partition by p_mfgr) as fv,
last_value(p_retailprice) over(partition by p_mfgr) as lv,
lead(p_retailprice) over(partition by p_mfgr) as lead1,
lag(p_retailprice) over(partition by p_mfgr) as lag1
from vector_ptf_part_simple_orc;


-- this now falls back to non-vectorized, "x following" should be handled in HIVE-24905
select "************ FOLLOWING ROWS NON-VECTORIZED REFERENCE ************";
set hive.vectorized.execution.ptf.enabled=false;
select p_mfgr, p_name, rowindex, p_date, p_retailprice,
count(*) over(partition by p_mfgr order by p_date range between 1 preceding and 3 following) as cs1,
count(*) over(partition by p_mfgr order by p_date range between 3 preceding and 1 following) as cs2,
count(rowindex) over(partition by p_mfgr order by p_date range between 1 preceding and 3 following) as c1,
count(rowindex) over(partition by p_mfgr order by p_date range between 3 preceding and 1 following) as c2,
count(p_date) over(partition by p_mfgr order by p_date range between 3 preceding and 1 following) as c_order,
sum(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and 3 following) as s1,
sum(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and 1 following) as s2,
min(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and 3 following) as min1,
min(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and 1 following) as min2,
max(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and 3 following) as max1,
max(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and 1 following) as max2,
avg(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and 3 following) as avg1,
avg(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and 1 following) as avg2,
row_number() over(partition by p_mfgr) as rn,
rank() over(partition by p_mfgr) as r,
dense_rank() over(partition by p_mfgr) as dr,
rank() over(partition by p_mfgr order by p_date) as r_date,
dense_rank() over(partition by p_mfgr order by p_date) as dr_date,
first_value(p_retailprice) over(partition by p_mfgr) as fv,
last_value(p_retailprice) over(partition by p_mfgr) as lv,
lead(p_retailprice) over(partition by p_mfgr) as lead1,
lag(p_retailprice) over(partition by p_mfgr) as lag1
from vector_ptf_part_simple_orc;

select "************ FOLLOWING ROWS ************";
set hive.vectorized.execution.ptf.enabled=true;
set hive.vectorized.ptf.max.memory.buffering.batch.count=2;
set hive.vectorized.testing.reducer.batch.size=2;

EXPLAIN VECTORIZATION DETAIL select p_mfgr, p_name, rowindex, p_date, p_retailprice,
count(*) over(partition by p_mfgr order by p_date range between 1 preceding and 3 following) as cs1,
count(*) over(partition by p_mfgr order by p_date range between 3 preceding and 1 following) as cs2,
count(rowindex) over(partition by p_mfgr order by p_date range between 1 preceding and 3 following) as c1,
count(rowindex) over(partition by p_mfgr order by p_date range between 3 preceding and 1 following) as c2,
count(p_date) over(partition by p_mfgr order by p_date range between 3 preceding and 1 following) as c_order,
sum(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and 3 following) as s1,
sum(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and 1 following) as s2,
min(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and 3 following) as min1,
min(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and 1 following) as min2,
max(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and 3 following) as max1,
max(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and 1 following) as max2,
avg(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and 3 following) as avg1,
avg(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and 1 following) as avg2,
row_number() over(partition by p_mfgr) as rn,
rank() over(partition by p_mfgr) as r,
dense_rank() over(partition by p_mfgr) as dr,
rank() over(partition by p_mfgr order by p_date) as r_date,
dense_rank() over(partition by p_mfgr order by p_date) as dr_date,
first_value(p_retailprice) over(partition by p_mfgr) as fv,
last_value(p_retailprice) over(partition by p_mfgr) as lv,
lead(p_retailprice) over(partition by p_mfgr) as lead1,
lag(p_retailprice) over(partition by p_mfgr) as lag1
from vector_ptf_part_simple_orc;

select p_mfgr, p_name, rowindex, p_date, p_retailprice,
count(*) over(partition by p_mfgr order by p_date range between 1 preceding and 3 following) as cs1,
count(*) over(partition by p_mfgr order by p_date range between 3 preceding and 1 following) as cs2,
count(rowindex) over(partition by p_mfgr order by p_date range between 1 preceding and 3 following) as c1,
count(rowindex) over(partition by p_mfgr order by p_date range between 3 preceding and 1 following) as c2,
count(p_date) over(partition by p_mfgr order by p_date range between 3 preceding and 1 following) as c_order,
sum(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and 3 following) as s1,
sum(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and 1 following) as s2,
min(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and 3 following) as min1,
min(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and 1 following) as min2,
max(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and 3 following) as max1,
max(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and 1 following) as max2,
avg(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and 3 following) as avg1,
avg(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and 1 following) as avg2,
row_number() over(partition by p_mfgr) as rn,
rank() over(partition by p_mfgr) as r,
dense_rank() over(partition by p_mfgr) as dr,
rank() over(partition by p_mfgr order by p_date) as r_date,
dense_rank() over(partition by p_mfgr order by p_date) as dr_date,
first_value(p_retailprice) over(partition by p_mfgr) as fv,
last_value(p_retailprice) over(partition by p_mfgr) as lv,
lead(p_retailprice) over(partition by p_mfgr) as lead1,
lag(p_retailprice) over(partition by p_mfgr) as lag1
from vector_ptf_part_simple_orc;


-- so far, we tested only date based range, it's time to quickly check all types
-- here are partition type checks as well, which are not closely related to ranges and boundaries,
-- but were seriously touched by changes in HIVE-24761
set hive.vectorized.ptf.max.memory.buffering.batch.count=2;
set hive.vectorized.testing.reducer.batch.size=2;





select "************ STRING WINDOW RANGE TYPE ************";
set hive.vectorized.execution.ptf.enabled=false;

select "************ STRING WINDOW RANGE TYPE (NON-VECTORIZED REFERENCE) ************";
select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice,
count(*) over(partition by p_mfgr order by p_name range between 3 preceding and current row) as cs,
sum(p_retailprice) over(partition by p_mfgr order by p_name range between 3 preceding and current row) as s
from vector_ptf_part_simple_orc;

select "************ STRING WINDOW RANGE TYPE (VECTORIZED) ************";
set hive.vectorized.execution.ptf.enabled=true;

EXPLAIN VECTORIZATION DETAIL select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice,
count(*) over(partition by p_mfgr order by p_name range between 3 preceding and current row) as cs,
sum(p_retailprice) over(partition by p_mfgr order by p_name range between 3 preceding and current row) as s
from vector_ptf_part_simple_orc;

select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice,
count(*) over(partition by p_mfgr order by p_name range between 3 preceding and current row) as cs,
sum(p_retailprice) over(partition by p_mfgr order by p_name range between 3 preceding and current row) as s
from vector_ptf_part_simple_orc;

select "************ STRING PARTITION ************";
EXPLAIN VECTORIZATION DETAIL select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice,
count(*) over(partition by p_name) as cs,
sum(p_retailprice) over(partition by p_name) as s
from vector_ptf_part_simple_orc;

select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice,
count(*) over(partition by p_name) as cs,
sum(p_retailprice) over(partition by p_name) as s
from vector_ptf_part_simple_orc;

select "************ STRING PARTITION WITH CONSTANT PARTITION EXPRESSION ************";
select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice,
count(*) over(partition by p_name) as cs,
sum(p_retailprice) over(partition by p_name) as s
from vector_ptf_part_simple_orc
where p_name = 'almond antique chartreuse lavender yellow';

EXPLAIN VECTORIZATION DETAIL select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice,
count(*) over(partition by p_name) as cs,
sum(p_retailprice) over(partition by p_name) as s
from vector_ptf_part_simple_orc
where p_name = 'almond antique chartreuse lavender yellow';



select "************ TIMESTAMP WINDOW RANGE TYPE ************";
set hive.vectorized.execution.ptf.enabled=false;

select "************ TIMESTAMP WINDOW RANGE TYPE (NON-VECTORIZED REFERENCE) ************";
select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice,
count(*) over(partition by p_mfgr order by p_timestamp range between 3 preceding and current row) as cs,
sum(p_retailprice) over(partition by p_mfgr order by p_timestamp range between 3 preceding and current row) as s
from vector_ptf_part_simple_orc;

select "************ TIMESTAMP WINDOW RANGE TYPE (VECTORIZED) ************";
set hive.vectorized.execution.ptf.enabled=true;

EXPLAIN VECTORIZATION DETAIL select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice,
count(*) over(partition by p_mfgr order by p_timestamp range between 3 preceding and current row) as cs,
sum(p_retailprice) over(partition by p_mfgr order by p_timestamp range between 3 preceding and current row) as s
from vector_ptf_part_simple_orc;

select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice,
count(*) over(partition by p_mfgr order by p_timestamp range between 3 preceding and current row) as cs,
sum(p_retailprice) over(partition by p_mfgr order by p_timestamp range between 3 preceding and current row) as s
from vector_ptf_part_simple_orc;

select "************ TIMESTAMP PARTITION ************";
EXPLAIN VECTORIZATION DETAIL select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice,
count(*) over(partition by p_timestamp) as cs,
sum(p_retailprice) over(partition by p_timestamp) as s
from vector_ptf_part_simple_orc;

select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice,
count(*) over(partition by p_timestamp) as cs,
sum(p_retailprice) over(partition by p_timestamp) as s
from vector_ptf_part_simple_orc;

select "************ TIMESTAMP PARTITION WITH CONSTANT PARTITION EXPRESSION ************";
EXPLAIN VECTORIZATION DETAIL select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice,
count(*) over(partition by p_timestamp) as cs,
sum(p_retailprice) over(partition by p_timestamp) as s
from vector_ptf_part_simple_orc
where p_timestamp = '1970-01-03 00:00:00.0';

select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice,
count(*) over(partition by p_timestamp) as cs,
sum(p_retailprice) over(partition by p_timestamp) as s
from vector_ptf_part_simple_orc
where p_timestamp = '1970-01-03 00:00:00.0';






select "************ DATE WINDOW RANGE TYPE ************";
set hive.vectorized.execution.ptf.enabled=false;

select "************ DATE WINDOW RANGE TYPE (NON-VECTORIZED REFERENCE) ************";
select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice,
count(*) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as cs,
sum(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as s
from vector_ptf_part_simple_orc;

select "************ DATE WINDOW RANGE TYPE (VECTORIZED) ************";
set hive.vectorized.execution.ptf.enabled=true;

EXPLAIN VECTORIZATION DETAIL select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice,
count(*) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as cs,
sum(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as s
from vector_ptf_part_simple_orc;

select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice,
count(*) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as cs,
sum(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as s
from vector_ptf_part_simple_orc;

select "************ DATE PARTITION ************";
EXPLAIN VECTORIZATION DETAIL select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice,
count(*) over(partition by p_date) as cs,
sum(p_retailprice) over(partition by p_date) as s
from vector_ptf_part_simple_orc;

select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice,
count(*) over(partition by p_date) as cs,
sum(p_retailprice) over(partition by p_date) as s
from vector_ptf_part_simple_orc;

select "************ DATE PARTITION WITH CONSTANT PARTITION EXPRESSION ************";
EXPLAIN VECTORIZATION DETAIL select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice,
count(*) over(partition by p_date) as cs,
sum(p_retailprice) over(partition by p_date) as s
from vector_ptf_part_simple_orc
where p_date = '1970-01-03';

select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice,
count(*) over(partition by p_date) as cs,
sum(p_retailprice) over(partition by p_date) as s
from vector_ptf_part_simple_orc
where p_date = '1970-01-03';







select "************ INT WINDOW RANGE TYPE ************";
set hive.vectorized.execution.ptf.enabled=false;

select "************ INT WINDOW RANGE TYPE (NON-VECTORIZED REFERENCE) ************";
select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice, p_int,
count(*) over(partition by p_mfgr order by p_int range between 3 preceding and current row) as cs,
sum(p_retailprice) over(partition by p_mfgr order by p_int range between 3 preceding and current row) as s
from vector_ptf_part_simple_orc;

select "************ INT WINDOW RANGE TYPE (VECTORIZED) ************";
set hive.vectorized.execution.ptf.enabled=true;

EXPLAIN VECTORIZATION DETAIL select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice, p_int,
count(*) over(partition by p_mfgr order by p_int range between 3 preceding and current row) as cs,
sum(p_retailprice) over(partition by p_mfgr order by p_int range between 3 preceding and current row) as s
from vector_ptf_part_simple_orc;

select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice, p_int,
count(*) over(partition by p_mfgr order by p_int range between 3 preceding and current row) as cs,
sum(p_retailprice) over(partition by p_mfgr order by p_int range between 3 preceding and current row) as s
from vector_ptf_part_simple_orc;

select "************ INT PARTITION ************";
EXPLAIN VECTORIZATION DETAIL select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice, p_int,
count(*) over(partition by p_int) as cs,
sum(p_retailprice) over(partition by p_int) as s
from vector_ptf_part_simple_orc;

select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice, p_int,
count(*) over(partition by p_int) as cs,
sum(p_retailprice) over(partition by p_int) as s
from vector_ptf_part_simple_orc;

select "************ INT PARTITION WITH CONSTANT PARTITION EXPRESSION ************";
EXPLAIN VECTORIZATION DETAIL select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice, p_int,
count(*) over(partition by p_int) as cs,
sum(p_retailprice) over(partition by p_int) as s
from vector_ptf_part_simple_orc
where p_date = 115200;

select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice,
count(*) over(partition by p_int) as cs,
sum(p_retailprice) over(partition by p_int) as s
from vector_ptf_part_simple_orc
where p_int = 115200;






select "************ DECIMAL WINDOW RANGE TYPE ************";
set hive.vectorized.execution.ptf.enabled=false;

select "************ DECIMAL WINDOW RANGE TYPE (NON-VECTORIZED REFERENCE) ************";
select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_decimal, p_retailprice,
count(*) over(partition by p_mfgr order by p_decimal range between 3 preceding and current row) as cs,
sum(p_retailprice) over(partition by p_mfgr order by p_decimal range between 3 preceding and current row) as s
from vector_ptf_part_simple_orc;

select "************ DECIMAL WINDOW RANGE TYPE (VECTORIZED) ************";
set hive.vectorized.execution.ptf.enabled=true;

EXPLAIN VECTORIZATION DETAIL select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_decimal, p_retailprice,
count(*) over(partition by p_mfgr order by p_decimal range between 3 preceding and current row) as cs,
sum(p_retailprice) over(partition by p_mfgr order by p_decimal range between 3 preceding and current row) as s
from vector_ptf_part_simple_orc;

select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_decimal, p_retailprice,
count(*) over(partition by p_mfgr order by p_decimal range between 3 preceding and current row) as cs,
sum(p_retailprice) over(partition by p_mfgr order by p_decimal range between 3 preceding and current row) as s
from vector_ptf_part_simple_orc;

select "************ DECIMAL PARTITION ************";
EXPLAIN VECTORIZATION DETAIL select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_decimal, p_retailprice,
count(*) over(partition by p_decimal) as cs,
sum(p_retailprice) over(partition by p_decimal) as s
from vector_ptf_part_simple_orc;

select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_decimal, p_retailprice,
count(*) over(partition by p_decimal) as cs,
sum(p_retailprice) over(partition by p_decimal) as s
from vector_ptf_part_simple_orc;

select "************ DECIMAL PARTITION WITH CONSTANT PARTITION EXPRESSION ************";
EXPLAIN VECTORIZATION DETAIL select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_decimal, p_retailprice,
count(*) over(partition by p_decimal) as cs,
sum(p_retailprice) over(partition by p_decimal) as s
from vector_ptf_part_simple_orc
where p_decimal = 1800.7000;

select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_decimal, p_retailprice,
count(*) over(partition by p_decimal) as cs,
sum(p_retailprice) over(partition by p_decimal) as s
from vector_ptf_part_simple_orc
where p_decimal = 1800.7000;


select "************ CHAR WINDOW RANGE TYPE ************";
set hive.vectorized.execution.ptf.enabled=false;

select "************ CHAR WINDOW RANGE TYPE (NON-VECTORIZED REFERENCE) ************";
select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice, p_char,
       count(*) over(partition by p_mfgr order by p_char range between 3 preceding and current row) as cs,
       sum(p_retailprice) over(partition by p_mfgr order by p_char range between 3 preceding and current row) as s
from vector_ptf_part_simple_orc;

select "************ CHAR WINDOW RANGE TYPE (VECTORIZED) ************";
set hive.vectorized.execution.ptf.enabled=true;

EXPLAIN VECTORIZATION DETAIL select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice,p_char,
                                    count(*) over(partition by p_mfgr order by p_char range between 3 preceding and
                                    current row) as cs,
                                    sum(p_retailprice) over(partition by p_mfgr order by p_char range between 3 preceding and current row) as s
from vector_ptf_part_simple_orc;

select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice,p_char,
       count(*) over(partition by p_mfgr order by p_char range between 3 preceding and current row) as cs,
       sum(p_retailprice) over(partition by p_mfgr order by p_char range between 3 preceding and current row) as s
from vector_ptf_part_simple_orc;

select "************ CHAR PARTITION ************";
EXPLAIN VECTORIZATION DETAIL select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice,p_char,
                                    count(*) over(partition by p_char) as cs,
                                    sum(p_retailprice) over(partition by p_char) as s
from vector_ptf_part_simple_orc;

select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice,p_char,
       count(*) over(partition by p_char) as cs,
       sum(p_retailprice) over(partition by p_char) as s
from vector_ptf_part_simple_orc;

select "************ CHAR PARTITION WITH CONSTANT PARTITION EXPRESSION ************";
EXPLAIN VECTORIZATION DETAIL select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice,p_char,
                                    count(*) over(partition by p_char) as cs,
                                    sum(p_retailprice) over(partition by p_char) as s
from vector_ptf_part_simple_orc
where p_char = 'D';

select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice, p_char,
       count(*) over(partition by p_char) as cs,
       sum(p_retailprice) over(partition by p_char) as s
from vector_ptf_part_simple_orc
where p_char = 'D';


select "************ VARCHAR WINDOW RANGE TYPE ************";
set hive.vectorized.execution.ptf.enabled=false;

select "************ VARCHAR WINDOW RANGE TYPE (NON-VECTORIZED REFERENCE) ************";
select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice,p_varchar,
       count(*) over(partition by p_mfgr order by p_varchar range between 3 preceding and current row) as cs,
       sum(p_retailprice) over(partition by p_mfgr order by p_varchar range between 3 preceding and current row) as s
from vector_ptf_part_simple_orc;

select "************ VARCHAR WINDOW RANGE TYPE (VECTORIZED) ************";
set hive.vectorized.execution.ptf.enabled=true;

EXPLAIN VECTORIZATION DETAIL select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice,p_varchar,
                                    count(*) over(partition by p_mfgr order by p_varchar range between 3 preceding and current row) as cs,
                                    sum(p_retailprice) over(partition by p_mfgr order by p_varchar range between 3 preceding and current row) as s
from vector_ptf_part_simple_orc;

select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice,p_varchar,
       count(*) over(partition by p_mfgr order by p_varchar range between 3 preceding and current row) as cs,
       sum(p_retailprice) over(partition by p_mfgr order by p_varchar range between 3 preceding and current row) as s
from vector_ptf_part_simple_orc;

select "************ VARCHAR PARTITION ************";
EXPLAIN VECTORIZATION DETAIL select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice,p_varchar,
                                    count(*) over(partition by p_varchar) as cs,
                                    sum(p_retailprice) over(partition by p_varchar) as s
from vector_ptf_part_simple_orc;

select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice,p_varchar,
       count(*) over(partition by p_varchar) as cs,
       sum(p_retailprice) over(partition by p_varchar) as s
from vector_ptf_part_simple_orc;

select "************ VARCHAR PARTITION WITH CONSTANT PARTITION EXPRESSION ************";
select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice,p_varchar,
       count(*) over(partition by p_varchar) as cs,
       sum(p_retailprice) over(partition by p_varchar) as s
from vector_ptf_part_simple_orc
where p_name = 'DA';

EXPLAIN VECTORIZATION DETAIL select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice, p_varchar,
                                    count(*) over(partition by p_varchar) as cs,
                                    sum(p_retailprice) over(partition by p_varchar) as s
from vector_ptf_part_simple_orc
where p_varchar = 'DA';

select "************ BOOLEAN WINDOW RANGE TYPE ************";
set hive.vectorized.execution.ptf.enabled=false;

select "************ BOOLEAN WINDOW RANGE TYPE (NON-VECTORIZED REFERENCE) ************";
select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice, p_char, p_boolean,
       count(*) over(partition by p_mfgr order by p_boolean range between 3 preceding and current row) as cs,
       sum(p_retailprice) over(partition by p_mfgr order by p_boolean range between 3 preceding and current row) as s
from vector_ptf_part_simple_orc;

select "************ BOOLEAN WINDOW RANGE TYPE (VECTORIZED) ************";
set hive.vectorized.execution.ptf.enabled=true;

EXPLAIN VECTORIZATION DETAIL select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice,p_char, p_boolean,
                                    count(*) over(partition by p_mfgr order by p_boolean range between 3 preceding and
                                    current row) as cs,
                                    sum(p_retailprice) over(partition by p_mfgr order by p_boolean range between 3 preceding and current row) as s
from vector_ptf_part_simple_orc;

select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice, p_char, p_boolean,
       count(*) over(partition by p_mfgr order by p_boolean range between 3 preceding and current row) as cs,
       sum(p_retailprice) over(partition by p_mfgr order by p_boolean range between 3 preceding and current row) as s
from vector_ptf_part_simple_orc;

select "************ BOOLEAN PARTITION ************";
EXPLAIN VECTORIZATION DETAIL select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice,p_char,p_boolean,
                                    count(*) over(partition by p_boolean) as cs,
                                    sum(p_retailprice) over(partition by p_boolean) as s
from vector_ptf_part_simple_orc;

select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice,p_char,p_boolean,
       count(*) over(partition by p_boolean) as cs,
       sum(p_retailprice) over(partition by p_boolean) as s
from vector_ptf_part_simple_orc;

select "************ BOOLEAN PARTITION WITH CONSTANT PARTITION EXPRESSION ************";
EXPLAIN VECTORIZATION DETAIL select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice,p_char,p_boolean,
                                    count(*) over(partition by p_boolean) as cs,
                                    sum(p_retailprice) over(partition by p_boolean) as s
from vector_ptf_part_simple_orc
where p_boolean = true;

select p_mfgr, p_name, p_timestamp, rowindex, p_date, p_retailprice, p_char,p_boolean,
       count(*) over(partition by p_boolean) as cs,
       sum(p_retailprice) over(partition by p_boolean) as s
from vector_ptf_part_simple_orc
where p_boolean = false;