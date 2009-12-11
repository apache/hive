EXPLAIN SELECT * FROM src LATERAL VIEW explode(array(1,2,3)) myTable AS myCol SORT BY key ASC, myCol ASC LIMIT 1;
EXPLAIN SELECT myTable.* FROM src LATERAL VIEW explode(array(1,2,3)) myTable AS myCol LIMIT 3;
EXPLAIN SELECT myTable.myCol, myTable2.myCol2 FROM src LATERAL VIEW explode(array(1,2,3)) myTable AS myCol LATERAL VIEW explode(array('a', 'b', 'c')) myTable2 AS myCol2 LIMIT 9;
EXPLAIN SELECT myTable2.* FROM src LATERAL VIEW explode(array(array(1,2,3))) myTable AS myCol LATERAL VIEW explode(myTable.myCol) myTable2 AS myCol2 LIMIT 3;

-- Verify that * selects columns from both tables
SELECT * FROM src LATERAL VIEW explode(array(1,2,3)) myTable AS myCol SORT BY key ASC, myCol ASC LIMIT 1;
-- TABLE.* should be supported
SELECT myTable.* FROM src LATERAL VIEW explode(array(1,2,3)) myTable AS myCol LIMIT 3;
-- Multiple lateral views should result in a Cartesian product
SELECT myTable.myCol, myTable2.myCol2 FROM src LATERAL VIEW explode(array(1,2,3)) myTable AS myCol LATERAL VIEW explode(array('a', 'b', 'c')) myTable2 AS myCol2 LIMIT 9;
-- Should be able to reference tables generated earlier
SELECT myTable2.* FROM src LATERAL VIEW explode(array(array(1,2,3))) myTable AS myCol LATERAL VIEW explode(myTable.myCol) myTable2 AS myCol2 LIMIT 3;
