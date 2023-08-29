SET hive.cli.print.header=true;

CREATE TABLE T1 (c_primitive int, c_array array<int>, c_nested array<struct<f1:int, f2:map<int, double>, f3:array<char(10)>>>);
CREATE TABLE T2 AS SELECT * FROM T1 LIMIT 0;
DESCRIBE FORMATTED t2;

create table t3 (a string, b string);

-- ctas with subquery and complex type
explain cbo
create table t_dest as
with cte1 as (select * from t3), cte2 as (select * from t1 where 1=0) select cte1.*, cte2.* from cte1 left join cte2;
create table t_dest as
with cte1 as (select * from t3), cte2 as (select * from t1 where 1=0) select cte1.*, cte2.* from cte1 left join cte2;
DESCRIBE FORMATTED t_dest;
