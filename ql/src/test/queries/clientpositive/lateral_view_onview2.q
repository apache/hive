CREATE TABLE lv_table( c1 STRING,  c2 ARRAY<INT>, c3 INT, c4 CHAR(1));
INSERT OVERWRITE TABLE lv_table SELECT 'abc  ', array(1,2,3), 100, 't' FROM src;

CREATE OR REPLACE VIEW lv_view AS SELECT * FROM lv_table; 

EXPLAIN SELECT myTable.myCol, myTable2.myCol2 FROM lv_view LATERAL VIEW explode(array(1,2,3)) myTable AS myCol LATERAL VIEW explode(array('a', 'b', 'c')) myTable2 AS myCol2 LIMIT 9;
