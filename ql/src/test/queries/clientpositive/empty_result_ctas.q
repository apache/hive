SET hive.cli.print.header=true;

CREATE TABLE T1 (c_primitive int, c_array array<int>, c_nested array<struct<f1:int, f2:map<int, double>, f3:array<char(10)>>>);
CREATE TABLE T2 AS SELECT * FROM T1 LIMIT 0;
DESCRIBE FORMATTED t2;
