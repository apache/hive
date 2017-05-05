CREATE TABLE lv_table( c1 STRING,  c2 ARRAY<INT>, c3 INT, c4 CHAR(1));
INSERT OVERWRITE TABLE lv_table SELECT 'abc  ', array(1,2,3), 100, 't' FROM src;

CREATE OR REPLACE VIEW lv_view AS SELECT * FROM lv_table; 

EXPLAIN SELECT * FROM lv_view LATERAL VIEW explode(array(1,2,3)) myTable AS myCol SORT BY c1 ASC, myCol ASC LIMIT 1;
EXPLAIN SELECT myTable.* FROM lv_view LATERAL VIEW explode(array(1,2,3)) myTable AS myCol LIMIT 3;
EXPLAIN SELECT myTable.myCol, myTable2.myCol2 FROM lv_view LATERAL VIEW explode(array(1,2,3)) myTable AS myCol LATERAL VIEW explode(array('a', 'b', 'c')) myTable2 AS myCol2 LIMIT 9;
EXPLAIN SELECT myTable2.* FROM lv_view LATERAL VIEW explode(array(array(1,2,3))) myTable AS myCol LATERAL VIEW explode(myTable.myCol) myTable2 AS myCol2 LIMIT 3;

-- Verify that * selects columns from both tables
SELECT * FROM lv_view LATERAL VIEW explode(array(1,2,3)) myTable AS myCol SORT BY c1 ASC, myCol ASC LIMIT 1;
-- TABLE.* should be supported
SELECT myTable.* FROM lv_view LATERAL VIEW explode(array(1,2,3)) myTable AS myCol LIMIT 3;
-- Multiple lateral views should result in a Cartesian product
SELECT myTable.myCol, myTable2.myCol2 FROM lv_view LATERAL VIEW explode(array(1,2,3)) myTable AS myCol LATERAL VIEW explode(array('a', 'b', 'c')) myTable2 AS myCol2 LIMIT 9;
-- Should be able to reference tables generated earlier
SELECT myTable2.* FROM lv_view LATERAL VIEW explode(array(array(1,2,3))) myTable AS myCol LATERAL VIEW explode(myTable.myCol) myTable2 AS myCol2 LIMIT 3;

EXPLAIN
SELECT SIZE(c2),c3,TRIM(c1),c4,myCol from lv_view LATERAL VIEW explode(array(1,2,3)) myTab as myCol limit 3;

SELECT SIZE(c2),c3,TRIM(c1),c4,myCol from lv_view LATERAL VIEW explode(array(1,2,3)) myTab as myCol limit 3;

CREATE TABLE lv_table1( c1 STRING,  c3 INT, c4 CHAR(1), c5 STRING, c6 STRING, c7 STRING, c8 STRING, c9 STRING, c10 STRING, c11 STRING, c12 STRING, c13 STRING);
CREATE TABLE lv_table2( c1 STRING,  c2 ARRAY<INT>);
INSERT OVERWRITE TABLE lv_table1 SELECT 'abc  ', 100, 't', 'test', 'test', 'test', 'test', 'test', 'test', 'test', 'test', 'test' FROM src;
INSERT OVERWRITE TABLE lv_table2 SELECT 'abc  ', array(1,2,3) FROM src;
EXPLAIN WITH lv_view1 AS (SELECT lv_table1.*, c2 FROM lv_table1 JOIN lv_table2 ON lv_table1.c1 = lv_table2.c1), lv_view2 AS (SELECT * FROM lv_view1 LATERAL VIEW explode(c2) myTable AS myCol) SELECT * FROM lv_view2 SORT BY c1 ASC, myCol ASC LIMIT 1;
WITH lv_view1 AS (SELECT lv_table1.*, c2 FROM lv_table1 JOIN lv_table2 ON lv_table1.c1 = lv_table2.c1), lv_view2 AS (SELECT * FROM lv_view1 LATERAL VIEW explode(c2) myTable AS myCol) SELECT * FROM lv_view2 SORT BY c1 ASC, myCol ASC LIMIT 1;