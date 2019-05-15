--! qt:dataset:src
CREATE TABLE lv_table1( c1 STRING,  c2 ARRAY<INT>, c3 INT, c4 CHAR(1), c5 STRING, c6 STRING, c7 STRING, c8 STRING, c9 STRING, c10 STRING, c11 STRING);
INSERT OVERWRITE TABLE lv_table1 SELECT 'abc  ', array(1,2,3), 100, 't', 'test', 'test', 'test', 'test', 'test', 'test', 'test' FROM src;
EXPLAIN SELECT * FROM lv_table1 LATERAL VIEW explode(array(1,2,3)) myTable AS myCol WHERE c3 = 100 SORT BY c1 ASC, myCol ASC LIMIT 1;
SELECT * FROM lv_table1 LATERAL VIEW explode(array(1,2,3)) myTable AS myCol WHERE c3 = 100 SORT BY c1 ASC, myCol ASC LIMIT 1;