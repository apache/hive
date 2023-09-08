--! qt:dataset:src
--Q1
EXPLAIN CBO SELECT myTable.myCol FROM src 
LATERAL VIEW explode(array(1,2,3)) myTable AS myCol;
--Q2
EXPLAIN CBO SELECT myTable.myCol, myTable2.myCol2 FROM src
LATERAL VIEW explode(array(1,2,3)) myTable AS myCol
LATERAL VIEW explode(array(1,2,3)) myTable2 AS myCol2;
