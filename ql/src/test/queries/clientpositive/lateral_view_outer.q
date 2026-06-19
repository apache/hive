--! qt:dataset:src
-- UDTF forwards nothing, OUTER LV add null for that
explain
select * from src LATERAL VIEW OUTER explode(array()) C AS a limit 10;
select * from src LATERAL VIEW OUTER explode(array()) C AS a limit 10;

-- backward compatible (UDTF forwards something for OUTER LV)
explain
select * from src LATERAL VIEW OUTER explode(array(4,5)) C AS a limit 10;
select * from src LATERAL VIEW OUTER explode(array(4,5)) C AS a limit 10;

create table array_valued as select key, if (key > 300, array(value, value), null) as value from src;

explain
select * from array_valued LATERAL VIEW OUTER explode(value) C AS a limit 10;
explain ast
select * from array_valued LATERAL VIEW OUTER explode(value) C AS a limit 10;
explain cbo
select * from array_valued LATERAL VIEW OUTER explode(value) C AS a limit 10;
select * from array_valued LATERAL VIEW OUTER explode(value) C AS a limit 10;

-- array_valued already has a nullable array column, which can be used for the view-based test
CREATE VIEW array_valued_view AS
SELECT array_valued.key AS key, a
FROM array_valued
LATERAL VIEW OUTER explode(value) lv AS a;

-- CBO plan should contain `outer=[true]` in HiveTableFunctionScan node.
EXPLAIN CBO
SELECT key, a FROM array_valued_view limit 10;
-- Explain plan should contain `outer lateral view: true` in the UDTF Operator
EXPLAIN
SELECT key, a FROM array_valued_view limit 10;
-- Rows with null array value should still appear with a=NULL
SELECT key, a FROM array_valued_view limit 10;
