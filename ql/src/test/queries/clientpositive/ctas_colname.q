-- SORT_QUERY_RESULTS

-- HIVE-4392, column aliases from expressionRR (GBY, etc.) are not valid name for table

-- group by


explain
create table summary as select *, key + 1, concat(value, value) from src limit 20;
create table summary as select *, key + 1, concat(value, value) from src limit 20;
describe formatted summary;
select * from summary;

-- window functions
explain
create table x4 as select *, rank() over(partition by key order by value) as rr from src1;
create table x4 as select *, rank() over(partition by key order by value) as rr from src1;
describe formatted x4;
select * from x4;

explain
create table x5 as select *, lead(key,1) over(partition by key order by value) as lead1 from src limit 20;
create table x5 as select *, lead(key,1) over(partition by key order by value) as lead1 from src limit 20;
describe formatted x5;
select * from x5;

-- sub queries
explain
create table x6 as select * from (select *, key + 1 from src1) a;
create table x6 as select * from (select *, key + 1 from src1) a;
describe formatted x6;
select * from x6;

explain
create table x7 as select * from (select *, count(value) from src group by key, value) a;
create table x7 as select * from (select *, count(value) from src group by key, value) a;
describe formatted x7;
select * from x7;

explain
create table x8 as select * from (select *, count(value) from src group by key, value having key < 9) a;
create table x8 as select * from (select *, count(value) from src group by key, value having key < 9) a;
describe formatted x8;
select * from x8;

explain
create table x9 as select * from (select max(value),key from src group by key having key < 9 AND max(value) IS NOT NULL) a;
create table x9 as select * from (select max(value),key from src group by key having key < 9 AND max(value) IS NOT NULL) a;
describe formatted x9;
select * from x9;

