SET hive.cli.print.header=true;

CREATE TABLE T1 (c_primitive int, c_array array<int>, c_nested array<struct<f1:int, f2:map<int, double>, f3:array<char(10)>>>);
CREATE TABLE T2 AS SELECT * FROM T1 LIMIT 0;
DESCRIBE FORMATTED t2;

-- empty source table
CREATE TABLE T3 AS SELECT * FROM T1;
DESCRIBE FORMATTED t3;

create table table1 (a string, b string);
create table table2 (complex_column array<struct<`family`:struct<`code`:string>, `values`:array<struct<`code`:string, `description`:string, `categories`:array<string>>>>>);

-- empty result subquery
create table table3 as with t1 as (select * from table1), t2 as (select * from table2 where 1=0) select t1.*, t2.* from t1 left join t2;

describe formatted table3;
