set hive.cli.print.header=true;
SET hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.ptf.enabled=true;
set hive.fetch.task.conversion=none;

create table vector_ptf_part_simple_text(p_mfgr string, p_name string, p_retailprice double)
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
        STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/vector_ptf_part_simple.txt' OVERWRITE INTO TABLE vector_ptf_part_simple_text;

create table vector_ptf_part_simple_orc(p_mfgr string, p_name string, p_retailprice double) stored as orc;
INSERT INTO TABLE vector_ptf_part_simple_orc SELECT * FROM vector_ptf_part_simple_text;

select * from vector_ptf_part_simple_orc;

-- ROW_NUMBER, RANK, DENSE_RANK, FIRST_VALUE, LAST_VALUE, COUNT, COUNT(*)

-- PARTITION BY

explain vectorization detail
select p_mfgr,p_name, p_retailprice,
row_number() over(partition by p_mfgr) as rn,
rank() over(partition by p_mfgr) as r,
dense_rank() over(partition by p_mfgr) as dr,
first_value(p_retailprice) over(partition by p_mfgr) as fv,
last_value(p_retailprice) over(partition by p_mfgr) as lv,
count(p_retailprice) over(partition by p_mfgr) as c,
count(*) over(partition by p_mfgr) as cs
from vector_ptf_part_simple_orc;

select p_mfgr,p_name, p_retailprice,
row_number() over(partition by p_mfgr) as rn,
rank() over(partition by p_mfgr) as r,
dense_rank() over(partition by p_mfgr) as dr,
first_value(p_retailprice) over(partition by p_mfgr) as fv,
last_value(p_retailprice) over(partition by p_mfgr) as lv,
count(p_retailprice) over(partition by p_mfgr) as c,
count(*) over(partition by p_mfgr) as cs
from vector_ptf_part_simple_orc;

-- RANGE

explain vectorization detail
select p_mfgr,p_name, p_retailprice,
row_number() over(partition by p_mfgr range between unbounded preceding and unbounded following) as rn,
rank() over(partition by p_mfgr range between unbounded preceding and unbounded following) as r,
dense_rank() over(partition by p_mfgr range between unbounded preceding and unbounded following) as dr,
first_value(p_retailprice) over(partition by p_mfgr range between unbounded preceding and current row) as fv,
last_value(p_retailprice) over(partition by p_mfgr range between unbounded preceding and current row) as lv,
count(p_retailprice) over(partition by p_mfgr range between unbounded preceding and current row) as c,
count(*) over(partition by p_mfgr range between unbounded preceding and current row) as cs
from vector_ptf_part_simple_orc;

select p_mfgr,p_name, p_retailprice,
row_number() over(partition by p_mfgr range between unbounded preceding and unbounded following) as rn,
rank() over(partition by p_mfgr range between unbounded preceding and unbounded following) as r,
dense_rank() over(partition by p_mfgr range between unbounded preceding and unbounded following) as dr,
first_value(p_retailprice) over(partition by p_mfgr range between unbounded preceding and current row) as fv,
last_value(p_retailprice) over(partition by p_mfgr range between unbounded preceding and current row) as lv,
count(p_retailprice) over(partition by p_mfgr range between unbounded preceding and current row) as c,
count(*) over(partition by p_mfgr range between unbounded preceding and current row) as cs
from vector_ptf_part_simple_orc;

-- ROWS

explain vectorization detail
select p_mfgr,p_name, p_retailprice,
row_number() over(partition by p_mfgr rows between unbounded preceding and unbounded following) as rn,
rank() over(partition by p_mfgr rows between unbounded preceding and unbounded following) as r,
dense_rank() over(partition by p_mfgr rows between unbounded preceding and unbounded following) as dr,
first_value(p_retailprice) over(partition by p_mfgr rows between unbounded preceding and current row) as fv,
last_value(p_retailprice) over(partition by p_mfgr rows between unbounded preceding and current row) as lv,
count(p_retailprice) over(partition by p_mfgr rows between unbounded preceding and current row) as c,
count(*) over(partition by p_mfgr rows between unbounded preceding and current row) as cs
from vector_ptf_part_simple_orc;

select p_mfgr,p_name, p_retailprice,
row_number() over(partition by p_mfgr rows between unbounded preceding and unbounded following) as rn,
rank() over(partition by p_mfgr rows between unbounded preceding and unbounded following) as r,
dense_rank() over(partition by p_mfgr rows between unbounded preceding and unbounded following) as dr,
first_value(p_retailprice) over(partition by p_mfgr rows between unbounded preceding and current row) as fv,
last_value(p_retailprice) over(partition by p_mfgr rows between unbounded preceding and current row) as lv,
count(p_retailprice) over(partition by p_mfgr rows between unbounded preceding and current row) as c,
count(*) over(partition by p_mfgr rows between unbounded preceding and current row) as cs
from vector_ptf_part_simple_orc;

-- PARTITION BY, ORDER BY

explain vectorization detail
select p_mfgr,p_name, p_retailprice,
row_number() over(partition by p_mfgr order by p_name) as rn,
rank() over(partition by p_mfgr order by p_name) as r,
dense_rank() over(partition by p_mfgr order by p_name) as dr,
first_value(p_retailprice) over(partition by p_mfgr order by p_name) as fv,
last_value(p_retailprice) over(partition by p_mfgr order by p_name) as lv,
count(p_retailprice) over(partition by p_mfgr order by p_name) as c,
count(*) over(partition by p_mfgr order by p_name) as cs
from vector_ptf_part_simple_orc;

select p_mfgr,p_name, p_retailprice,
row_number() over(partition by p_mfgr order by p_name) as rn,
rank() over(partition by p_mfgr order by p_name) as r,
dense_rank() over(partition by p_mfgr order by p_name) as dr,
first_value(p_retailprice) over(partition by p_mfgr order by p_name) as fv,
last_value(p_retailprice) over(partition by p_mfgr order by p_name) as lv,
count(p_retailprice) over(partition by p_mfgr order by p_name) as c,
count(*) over(partition by p_mfgr order by p_name) as cs
from vector_ptf_part_simple_orc;

-- RANGE

explain vectorization detail
select p_mfgr,p_name, p_retailprice,
row_number() over(partition by p_mfgr order by p_name range between unbounded preceding and unbounded following) as rn,
rank() over(partition by p_mfgr order by p_name range between unbounded preceding and unbounded following) as r,
dense_rank() over(partition by p_mfgr order by p_name range between unbounded preceding and unbounded following) as dr,
first_value(p_retailprice) over(partition by p_mfgr order by p_name range between unbounded preceding and current row) as fv,
last_value(p_retailprice) over(partition by p_mfgr order by p_name range between unbounded preceding and current row) as lv,
count(p_retailprice) over(partition by p_mfgr order by p_name range between unbounded preceding and current row) as c,
count(*) over(partition by p_mfgr order by p_name range between unbounded preceding and current row) as cs
from vector_ptf_part_simple_orc;

select p_mfgr,p_name, p_retailprice,
row_number() over(partition by p_mfgr order by p_name range between unbounded preceding and unbounded following) as rn,
rank() over(partition by p_mfgr order by p_name range between unbounded preceding and unbounded following) as r,
dense_rank() over(partition by p_mfgr order by p_name range between unbounded preceding and unbounded following) as dr,
first_value(p_retailprice) over(partition by p_mfgr order by p_name range between unbounded preceding and current row) as fv,
last_value(p_retailprice) over(partition by p_mfgr order by p_name range between unbounded preceding and current row) as lv,
count(p_retailprice) over(partition by p_mfgr order by p_name range between unbounded preceding and current row) as c,
count(*) over(partition by p_mfgr order by p_name range between unbounded preceding and current row) as cs
from vector_ptf_part_simple_orc;

-- ROWS

explain vectorization detail
select p_mfgr,p_name, p_retailprice,
row_number() over(partition by p_mfgr order by p_name rows between unbounded preceding and unbounded following) as rn,
rank() over(partition by p_mfgr order by p_name rows between unbounded preceding and unbounded following) as r,
dense_rank() over(partition by p_mfgr order by p_name rows between unbounded preceding and unbounded following) as dr,
first_value(p_retailprice) over(partition by p_mfgr order by p_name rows between unbounded preceding and current row) as fv,
last_value(p_retailprice) over(partition by p_mfgr order by p_name rows between unbounded preceding and current row) as lv,
count(p_retailprice) over(partition by p_mfgr order by p_name rows between unbounded preceding and current row) as c,
count(*) over(partition by p_mfgr order by p_name rows between unbounded preceding and current row) as cs
from vector_ptf_part_simple_orc;

select p_mfgr,p_name, p_retailprice,
row_number() over(partition by p_mfgr order by p_name rows between unbounded preceding and unbounded following) as rn,
rank() over(partition by p_mfgr order by p_name rows between unbounded preceding and unbounded following) as r,
dense_rank() over(partition by p_mfgr order by p_name rows between unbounded preceding and unbounded following) as dr,
first_value(p_retailprice) over(partition by p_mfgr order by p_name rows between unbounded preceding and current row) as fv,
last_value(p_retailprice) over(partition by p_mfgr order by p_name rows between unbounded preceding and current row) as lv,
count(p_retailprice) over(partition by p_mfgr order by p_name rows between unbounded preceding and current row) as c,
count(*) over(partition by p_mfgr order by p_name rows between unbounded preceding and current row) as cs
from vector_ptf_part_simple_orc;

-- ORDER BY

explain vectorization detail
select p_mfgr,p_name, p_retailprice,
row_number() over(order by p_name) as rn,
rank() over(order by p_name) as r,
dense_rank() over(order by p_name) as dr,
first_value(p_retailprice) over(order by p_name) as fv,
last_value(p_retailprice) over(order by p_name) as lv,
count(p_retailprice) over(order by p_name) as c,
count(*) over(order by p_name) as cs
from vector_ptf_part_simple_orc;

select p_mfgr,p_name, p_retailprice,
row_number() over(order by p_name) as rn,
rank() over(order by p_name) as r,
dense_rank() over(order by p_name) as dr,
first_value(p_retailprice) over(order by p_name) as fv,
last_value(p_retailprice) over(order by p_name) as lv,
count(p_retailprice) over(order by p_name) as c,
count(*) over(order by p_name) as cs
from vector_ptf_part_simple_orc;

-- RANGE

explain vectorization detail
select p_mfgr,p_name, p_retailprice,
row_number() over(order by p_name range between unbounded preceding and unbounded following) as rn,
rank() over(order by p_name range between unbounded preceding and unbounded following) as r,
dense_rank() over(order by p_name range between unbounded preceding and unbounded following) as dr,
first_value(p_retailprice) over(order by p_name range between unbounded preceding and current row) as fv,
last_value(p_retailprice) over(order by p_name range between unbounded preceding and current row) as lv,
count(p_retailprice) over(order by p_name range between unbounded preceding and current row) as c,
count(*) over(order by p_name range between unbounded preceding and current row) as cs
from vector_ptf_part_simple_orc;

select p_mfgr,p_name, p_retailprice,
row_number() over(order by p_name range between unbounded preceding and unbounded following) as rn,
rank() over(order by p_name range between unbounded preceding and unbounded following) as r,
dense_rank() over(order by p_name range between unbounded preceding and unbounded following) as dr,
first_value(p_retailprice) over(order by p_name range between unbounded preceding and current row) as fv,
last_value(p_retailprice) over(order by p_name range between unbounded preceding and current row) as lv,
count(p_retailprice) over(order by p_name range between unbounded preceding and current row) as c,
count(*) over(order by p_name range between unbounded preceding and current row) as cs
from vector_ptf_part_simple_orc;

-- ROWS

explain vectorization detail
select p_mfgr,p_name, p_retailprice,
row_number() over(order by p_name rows between unbounded preceding and unbounded following) as rn,
rank() over(order by p_name rows between unbounded preceding and unbounded following) as r,
dense_rank() over(order by p_name rows between unbounded preceding and unbounded following) as dr,
first_value(p_retailprice) over(order by p_name rows between unbounded preceding and current row) as fv,
last_value(p_retailprice) over(order by p_name rows between unbounded preceding and current row) as lv,
count(p_retailprice) over(order by p_name rows between unbounded preceding and current row) as c,
count(*) over(order by p_name rows between unbounded preceding and current row) as cs
from vector_ptf_part_simple_orc;

select p_mfgr,p_name, p_retailprice,
row_number() over(order by p_name rows between unbounded preceding and unbounded following) as rn,
rank() over(order by p_name rows between unbounded preceding and unbounded following) as r,
dense_rank() over(order by p_name rows between unbounded preceding and unbounded following) as dr,
first_value(p_retailprice) over(order by p_name rows between unbounded preceding and current row) as fv,
last_value(p_retailprice) over(order by p_name rows between unbounded preceding and current row) as lv,
count(p_retailprice) over(order by p_name rows between unbounded preceding and current row) as c,
count(*) over(order by p_name rows between unbounded preceding and current row) as cs
from vector_ptf_part_simple_orc;

-- SUM, MIN, MAX, avg

-- PARTITION BY

explain vectorization detail
select p_mfgr,p_name, p_retailprice,
sum(p_retailprice) over(partition by p_mfgr) as s,
min(p_retailprice) over(partition by p_mfgr) as mi,
max(p_retailprice) over(partition by p_mfgr) as ma,
avg(p_retailprice) over(partition by p_mfgr) as av 
from vector_ptf_part_simple_orc;

select p_mfgr,p_name, p_retailprice,
sum(p_retailprice) over(partition by p_mfgr) as s,
min(p_retailprice) over(partition by p_mfgr) as mi,
max(p_retailprice) over(partition by p_mfgr) as ma,
avg(p_retailprice) over(partition by p_mfgr) as av 
from vector_ptf_part_simple_orc;

-- RANGE

explain vectorization detail
select p_mfgr,p_name, p_retailprice,
sum(p_retailprice) over(partition by p_mfgr range between unbounded preceding and current row) as s,
min(p_retailprice) over(partition by p_mfgr range between unbounded preceding and current row) as mi,
max(p_retailprice) over(partition by p_mfgr range between unbounded preceding and current row) as ma,
avg(p_retailprice) over(partition by p_mfgr range between unbounded preceding and current row) as av 
from vector_ptf_part_simple_orc;

select p_mfgr,p_name, p_retailprice,
sum(p_retailprice) over(partition by p_mfgr range between unbounded preceding and current row) as s,
min(p_retailprice) over(partition by p_mfgr range between unbounded preceding and current row) as mi,
max(p_retailprice) over(partition by p_mfgr range between unbounded preceding and current row) as ma,
avg(p_retailprice) over(partition by p_mfgr range between unbounded preceding and current row) as av 
from vector_ptf_part_simple_orc;


-- ROW

explain vectorization detail
select p_mfgr,p_name, p_retailprice,
sum(p_retailprice) over(partition by p_mfgr rows between unbounded preceding and current row) as s,
min(p_retailprice) over(partition by p_mfgr rows between unbounded preceding and current row) as mi,
max(p_retailprice) over(partition by p_mfgr rows between unbounded preceding and current row) as ma,
avg(p_retailprice) over(partition by p_mfgr rows between unbounded preceding and current row) as av 
from vector_ptf_part_simple_orc;

select p_mfgr,p_name, p_retailprice,
sum(p_retailprice) over(partition by p_mfgr rows between unbounded preceding and current row) as s,
min(p_retailprice) over(partition by p_mfgr rows between unbounded preceding and current row) as mi,
max(p_retailprice) over(partition by p_mfgr rows between unbounded preceding and current row) as ma,
avg(p_retailprice) over(partition by p_mfgr rows between unbounded preceding and current row) as av 
from vector_ptf_part_simple_orc;

-- PARTITION BY, ORDER BY

explain vectorization detail
select p_mfgr,p_name, p_retailprice,
sum(p_retailprice) over(partition by p_mfgr order by p_name) as s,
min(p_retailprice) over(partition by p_mfgr order by p_name) as mi,
max(p_retailprice) over(partition by p_mfgr order by p_name) as ma,
avg(p_retailprice) over(partition by p_mfgr order by p_name) as av 
from vector_ptf_part_simple_orc;

select p_mfgr,p_name, p_retailprice,
sum(p_retailprice) over(partition by p_mfgr order by p_name) as s,
min(p_retailprice) over(partition by p_mfgr order by p_name) as mi,
max(p_retailprice) over(partition by p_mfgr order by p_name) as ma,
avg(p_retailprice) over(partition by p_mfgr order by p_name) as av 
from vector_ptf_part_simple_orc;

-- RANGE

explain vectorization detail
select p_mfgr,p_name, p_retailprice,
sum(p_retailprice) over(partition by p_mfgr order by p_name range between unbounded preceding and current row) as s,
min(p_retailprice) over(partition by p_mfgr order by p_name range between unbounded preceding and current row) as mi,
max(p_retailprice) over(partition by p_mfgr order by p_name range between unbounded preceding and current row) as ma,
avg(p_retailprice) over(partition by p_mfgr order by p_name range between unbounded preceding and current row) as av 
from vector_ptf_part_simple_orc;

select p_mfgr,p_name, p_retailprice,
sum(p_retailprice) over(partition by p_mfgr order by p_name range between unbounded preceding and current row) as s,
min(p_retailprice) over(partition by p_mfgr order by p_name range between unbounded preceding and current row) as mi,
max(p_retailprice) over(partition by p_mfgr order by p_name range between unbounded preceding and current row) as ma,
avg(p_retailprice) over(partition by p_mfgr order by p_name range between unbounded preceding and current row) as av 
from vector_ptf_part_simple_orc;


-- ROW

explain vectorization detail
select p_mfgr,p_name, p_retailprice,
sum(p_retailprice) over(partition by p_mfgr order by p_name rows between unbounded preceding and current row) as s,
min(p_retailprice) over(partition by p_mfgr order by p_name rows between unbounded preceding and current row) as mi,
max(p_retailprice) over(partition by p_mfgr order by p_name rows between unbounded preceding and current row) as ma,
avg(p_retailprice) over(partition by p_mfgr order by p_name rows between unbounded preceding and current row) as av 
from vector_ptf_part_simple_orc;

select p_mfgr,p_name, p_retailprice,
sum(p_retailprice) over(partition by p_mfgr order by p_name rows between unbounded preceding and current row) as s,
min(p_retailprice) over(partition by p_mfgr order by p_name rows between unbounded preceding and current row) as mi,
max(p_retailprice) over(partition by p_mfgr order by p_name rows between unbounded preceding and current row) as ma,
avg(p_retailprice) over(partition by p_mfgr order by p_name rows between unbounded preceding and current row) as av 
from vector_ptf_part_simple_orc;

-- PARTITION BY, ORDER BY

explain vectorization detail
select p_mfgr,p_name, p_retailprice,
sum(p_retailprice) over(order by p_name) as s,
min(p_retailprice) over(order by p_name) as mi,
max(p_retailprice) over(order by p_name) as ma,
avg(p_retailprice) over(order by p_name) as av 
from vector_ptf_part_simple_orc;

select p_mfgr,p_name, p_retailprice,
sum(p_retailprice) over(order by p_name) as s,
min(p_retailprice) over(order by p_name) as mi,
max(p_retailprice) over(order by p_name) as ma,
avg(p_retailprice) over(order by p_name) as av 
from vector_ptf_part_simple_orc;

-- RANGE

explain vectorization detail
select p_mfgr,p_name, p_retailprice,
sum(p_retailprice) over(order by p_name range between unbounded preceding and current row) as s,
min(p_retailprice) over(order by p_name range between unbounded preceding and current row) as mi,
max(p_retailprice) over(order by p_name range between unbounded preceding and current row) as ma,
avg(p_retailprice) over(order by p_name range between unbounded preceding and current row) as av 
from vector_ptf_part_simple_orc;

select p_mfgr,p_name, p_retailprice,
sum(p_retailprice) over(order by p_name range between unbounded preceding and current row) as s,
min(p_retailprice) over(order by p_name range between unbounded preceding and current row) as mi,
max(p_retailprice) over(order by p_name range between unbounded preceding and current row) as ma,
avg(p_retailprice) over(order by p_name range between unbounded preceding and current row) as av 
from vector_ptf_part_simple_orc;


-- ROW

explain vectorization detail
select p_mfgr,p_name, p_retailprice,
sum(p_retailprice) over(order by p_name rows between unbounded preceding and current row) as s,
min(p_retailprice) over(order by p_name rows between unbounded preceding and current row) as mi,
max(p_retailprice) over(order by p_name rows between unbounded preceding and current row) as ma,
avg(p_retailprice) over(order by p_name rows between unbounded preceding and current row) as av 
from vector_ptf_part_simple_orc;

select p_mfgr,p_name, p_retailprice,
sum(p_retailprice) over(order by p_name rows between unbounded preceding and current row) as s,
min(p_retailprice) over(order by p_name rows between unbounded preceding and current row) as mi,
max(p_retailprice) over(order by p_name rows between unbounded preceding and current row) as ma,
avg(p_retailprice) over(order by p_name rows between unbounded preceding and current row) as av 
from vector_ptf_part_simple_orc;

-- DECIMAL

create table vector_ptf_part_simple_text_decimal(p_mfgr string, p_name string, p_retailprice decimal(38,18))
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
        STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/vector_ptf_part_simple.txt' OVERWRITE INTO TABLE vector_ptf_part_simple_text_decimal;

create table vector_ptf_part_simple_orc_decimal(p_mfgr string, p_name string, p_retailprice decimal(38,18)) stored as orc;
INSERT INTO TABLE vector_ptf_part_simple_orc_decimal SELECT * FROM vector_ptf_part_simple_text_decimal;

explain vectorization detail
select p_mfgr,p_name, p_retailprice,
sum(p_retailprice) over(partition by p_mfgr) as s,
min(p_retailprice) over(partition by p_mfgr) as mi,
max(p_retailprice) over(partition by p_mfgr) as ma,
avg(p_retailprice) over(partition by p_mfgr) as av 
from vector_ptf_part_simple_orc_decimal;

select p_mfgr,p_name, p_retailprice,
sum(p_retailprice) over(partition by p_mfgr) as s,
min(p_retailprice) over(partition by p_mfgr) as mi,
max(p_retailprice) over(partition by p_mfgr) as ma,
avg(p_retailprice) over(partition by p_mfgr) as av 
from vector_ptf_part_simple_orc_decimal;


explain vectorization detail
select p_mfgr,p_name, p_retailprice,
sum(p_retailprice) over(partition by p_mfgr order by p_name) as s,
min(p_retailprice) over(partition by p_mfgr order by p_name) as mi,
max(p_retailprice) over(partition by p_mfgr order by p_name) as ma,
avg(p_retailprice) over(partition by p_mfgr order by p_name) as av 
from vector_ptf_part_simple_orc_decimal;

select p_mfgr,p_name, p_retailprice,
sum(p_retailprice) over(partition by p_mfgr order by p_name) as s,
min(p_retailprice) over(partition by p_mfgr order by p_name) as mi,
max(p_retailprice) over(partition by p_mfgr order by p_name) as ma,
avg(p_retailprice) over(partition by p_mfgr order by p_name) as av 
from vector_ptf_part_simple_orc_decimal;




create table vector_ptf_part_simple_orc_long(p_mfgr string, p_name string, p_bigint bigint) stored as orc;
INSERT INTO TABLE vector_ptf_part_simple_orc_long SELECT p_mfgr, p_name, cast(p_retailprice * 100 as bigint) FROM vector_ptf_part_simple_text_decimal;

explain vectorization detail
select p_mfgr,p_name, p_bigint,
sum(p_bigint) over(partition by p_mfgr) as s,
min(p_bigint) over(partition by p_mfgr) as mi,
max(p_bigint) over(partition by p_mfgr) as ma,
avg(p_bigint) over(partition by p_mfgr) as av 
from vector_ptf_part_simple_orc_long;

select p_mfgr,p_name, p_bigint,
sum(p_bigint) over(partition by p_mfgr) as s,
min(p_bigint) over(partition by p_mfgr) as mi,
max(p_bigint) over(partition by p_mfgr) as ma,
avg(p_bigint) over(partition by p_mfgr) as av 
from vector_ptf_part_simple_orc_long;


explain vectorization detail
select p_mfgr,p_name, p_bigint,
sum(p_bigint) over(partition by p_mfgr order by p_name) as s,
min(p_bigint) over(partition by p_mfgr order by p_name) as mi,
max(p_bigint) over(partition by p_mfgr order by p_name) as ma,
avg(p_bigint) over(partition by p_mfgr order by p_name) as av 
from vector_ptf_part_simple_orc_long;

select p_mfgr,p_name, p_bigint,
sum(p_bigint) over(partition by p_mfgr order by p_name) as s,
min(p_bigint) over(partition by p_mfgr order by p_name) as mi,
max(p_bigint) over(partition by p_mfgr order by p_name) as ma,
avg(p_bigint) over(partition by p_mfgr order by p_name) as av 
from vector_ptf_part_simple_orc_long;


-- Omit p_name columns

explain vectorization detail
select p_mfgr, p_retailprice,
rank() over(partition by p_mfgr) as r
from vector_ptf_part_simple_orc;

select p_mfgr, p_retailprice,
rank() over(partition by p_mfgr) as r
from vector_ptf_part_simple_orc;


explain vectorization detail
select p_mfgr, p_retailprice,
rank() over(partition by p_mfgr order by p_name) as r
from vector_ptf_part_simple_orc;

select p_mfgr, p_retailprice,
rank() over(partition by p_mfgr order by p_name) as r
from vector_ptf_part_simple_orc;


-- Calculated partition key

explain vectorization detail
select p_mfgr, p_name, p_retailprice,
rank() over(partition by p_mfgr, case when p_mfgr == "Manufacturer#2" then timestamp "2000-01-01 00:00:00" end) as r
from vector_ptf_part_simple_orc;

select p_mfgr, p_name, p_retailprice,
rank() over(partition by p_mfgr, case when p_mfgr == "Manufacturer#2" then timestamp "2000-01-01 00:00:00" end) as r
from vector_ptf_part_simple_orc;

explain vectorization detail
select p_mfgr, p_name, p_retailprice,
rank() over(partition by p_mfgr, case when p_mfgr == "Manufacturer#2" then timestamp "2000-01-01 00:00:00" end order by p_name) as r
from vector_ptf_part_simple_orc;

select p_mfgr, p_name, p_retailprice,
rank() over(partition by p_mfgr, case when p_mfgr == "Manufacturer#2" then timestamp "2000-01-01 00:00:00" end order by p_name) as r
from vector_ptf_part_simple_orc;


--
-- Run some tests with these parameters that force spilling to disk.
--
set hive.vectorized.ptf.max.memory.buffering.batch.count=1;
set hive.vectorized.testing.reducer.batch.size=2;

select p_mfgr,p_name, p_retailprice,
row_number() over(partition by p_mfgr) as rn,
rank() over(partition by p_mfgr) as r,
dense_rank() over(partition by p_mfgr) as dr,
first_value(p_retailprice) over(partition by p_mfgr) as fv,
last_value(p_retailprice) over(partition by p_mfgr) as lv,
count(p_retailprice) over(partition by p_mfgr) as c,
count(*) over(partition by p_mfgr) as cs
from vector_ptf_part_simple_orc;

select p_mfgr,p_name, p_retailprice,
row_number() over(partition by p_mfgr order by p_name) as rn,
rank() over(partition by p_mfgr order by p_name) as r,
dense_rank() over(partition by p_mfgr order by p_name) as dr,
first_value(p_retailprice) over(partition by p_mfgr order by p_name) as fv,
last_value(p_retailprice) over(partition by p_mfgr order by p_name) as lv,
count(p_retailprice) over(partition by p_mfgr order by p_name) as c,
count(*) over(partition by p_mfgr order by p_name) as cs
from vector_ptf_part_simple_orc;


explain vectorization detail
select p_mfgr, p_retailprice,
rank() over(partition by p_mfgr) as r
from vector_ptf_part_simple_orc;

select p_mfgr, p_retailprice,
rank() over(partition by p_mfgr) as r
from vector_ptf_part_simple_orc;

explain vectorization detail
select p_mfgr, p_retailprice,
rank() over(partition by p_mfgr order by p_name) as r
from vector_ptf_part_simple_orc;

select p_mfgr, p_retailprice,
rank() over(partition by p_mfgr order by p_name) as r
from vector_ptf_part_simple_orc;

explain vectorization detail
select p_mfgr, p_name, p_retailprice,
rank() over(partition by p_mfgr, case when p_mfgr == "Manufacturer#2" then timestamp "2000-01-01 00:00:00" end order by p_name) as r
from vector_ptf_part_simple_orc;

select p_mfgr, p_name, p_retailprice,
rank() over(partition by p_mfgr, case when p_mfgr == "Manufacturer#2" then timestamp "2000-01-01 00:00:00" end order by p_name) as r
from vector_ptf_part_simple_orc;

explain vectorization detail
select p_mfgr, p_name, p_retailprice,
rank() over(partition by p_mfgr, case when p_mfgr == "Manufacturer#2" then timestamp "2000-01-01 00:00:00" end) as r
from vector_ptf_part_simple_orc;

select p_mfgr, p_name, p_retailprice,
rank() over(partition by p_mfgr, case when p_mfgr == "Manufacturer#2" then timestamp "2000-01-01 00:00:00" end) as r
from vector_ptf_part_simple_orc;

