DROP VIEW view1;
DROP VIEW view2;
DROP VIEW view3;
DROP VIEW view4;
DROP VIEW view5;
DROP VIEW view6;
DROP VIEW view7;
DROP VIEW view8;
DROP VIEW view9;
DROP VIEW view10;
DROP VIEW view11;
DROP VIEW view12;
DROP VIEW view13;
DROP VIEW view14;
DROP TEMPORARY FUNCTION test_translate;
DROP TEMPORARY FUNCTION test_max;
DROP TEMPORARY FUNCTION test_explode;
DROP TABLE table1;

SELECT * FROM src WHERE key=86;
CREATE VIEW view1 AS SELECT value FROM src WHERE key=86;
CREATE VIEW view2 AS SELECT * FROM src;
CREATE VIEW view3(valoo) AS SELECT upper(value) FROM src WHERE key=86;
SELECT * from view1;
SELECT * from view2 where key=18;
SELECT * from view3;

-- test EXPLAIN output for CREATE VIEW
EXPLAIN
CREATE VIEW view0(valoo) AS SELECT upper(value) FROM src WHERE key=86;

-- make sure EXPLAIN works with a query which references a view
EXPLAIN
SELECT * from view2 where key=18;

SHOW TABLES 'view.*';
DESCRIBE view1;
DESCRIBE EXTENDED view1;
DESCRIBE view2;
DESCRIBE EXTENDED view2;
DESCRIBE view3;
DESCRIBE EXTENDED view3;

CREATE TABLE table1 (key int);

INSERT OVERWRITE TABLE table1 SELECT key FROM src WHERE key = 86;

SELECT * FROM table1;
CREATE VIEW view4 AS SELECT * FROM table1;
SELECT * FROM view4;
DESCRIBE view4;
ALTER TABLE table1 ADD COLUMNS (value STRING);
SELECT * FROM table1;
SELECT * FROM view4;
DESCRIBE table1;
DESCRIBE view4;

CREATE VIEW view5 AS SELECT v1.key as key1, v2.key as key2
FROM view4 v1 join view4 v2;
SELECT * FROM view5;
DESCRIBE view5;

-- verify that column name and comment in DDL portion 
-- overrides column alias in SELECT
CREATE VIEW view6(valoo COMMENT 'I cannot spell') AS 
SELECT upper(value) as blarg FROM src WHERE key=86;
DESCRIBE view6;

-- verify that ORDER BY and LIMIT are both supported in view def
CREATE VIEW view7 AS
SELECT * FROM src
WHERE key > 80 AND key < 100
ORDER BY key, value
LIMIT 10;

SELECT * FROM view7;

-- top-level ORDER BY should override the one inside the view
-- (however, the inside ORDER BY should still influence the evaluation
-- of the limit)
SELECT * FROM view7 ORDER BY key DESC, value;

-- top-level LIMIT should override if lower
SELECT * FROM view7 LIMIT 5;

-- but not if higher
SELECT * FROM view7 LIMIT 20;

-- test usage of a function within a view
CREATE TEMPORARY FUNCTION test_translate AS 
'org.apache.hadoop.hive.ql.udf.generic.GenericUDFTestTranslate';
CREATE VIEW view8(c) AS
SELECT test_translate('abc', 'a', 'b') 
FROM table1;
DESCRIBE EXTENDED view8;
SELECT * FROM view8;

-- test usage of a UDAF within a view
CREATE TEMPORARY FUNCTION test_max AS 
'org.apache.hadoop.hive.ql.udf.UDAFTestMax';
CREATE VIEW view9(m) AS
SELECT test_max(length(value))
FROM src;
DESCRIBE EXTENDED view9;
SELECT * FROM view9;

-- test usage of a subselect within a view
CREATE VIEW view10 AS
SELECT slurp.* FROM (SELECT * FROM src WHERE key=86) slurp;
DESCRIBE EXTENDED view10;
SELECT * FROM view10;

-- test usage of a UDTF within a view
CREATE TEMPORARY FUNCTION test_explode AS 
'org.apache.hadoop.hive.ql.udf.generic.GenericUDTFExplode';
CREATE VIEW view11 AS
SELECT test_explode(array(1,2,3)) AS (boom)
FROM table1;
DESCRIBE EXTENDED view11;
SELECT * FROM view11;

-- test usage of LATERAL within a view
CREATE VIEW view12 AS
SELECT * FROM src LATERAL VIEW explode(array(1,2,3)) myTable AS myCol;
DESCRIBE EXTENDED view12;
SELECT * FROM view12
ORDER BY key ASC, myCol ASC LIMIT 1;

-- test usage of LATERAL with a view as the LHS
SELECT * FROM view2 LATERAL VIEW explode(array(1,2,3)) myTable AS myCol
ORDER BY key ASC, myCol ASC LIMIT 1;

-- test usage of TABLESAMPLE within a view
CREATE VIEW view13 AS
SELECT s.key
FROM srcbucket TABLESAMPLE (BUCKET 1 OUT OF 5 ON key) s;
DESCRIBE EXTENDED view13;
SELECT * FROM view13
ORDER BY key LIMIT 12;

-- test usage of JOIN+UNION+AGG all within same view
CREATE VIEW view14 AS
SELECT unionsrc1.key as k1, unionsrc1.value as v1, 
       unionsrc2.key as k2, unionsrc2.value as v2
FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1
                         UNION  ALL  
      select s2.key as key, s2.value as value from src s2 where s2.key < 10) unionsrc1 
JOIN 
     (select 'tst1' as key, cast(count(1) as string) as value from src s3
                         UNION  ALL  
      select s4.key as key, s4.value as value from src s4 where s4.key < 10) unionsrc2
ON (unionsrc1.key = unionsrc2.key);
DESCRIBE EXTENDED view14;
SELECT * FROM view14
ORDER BY k1;

-- this should work since currently we don't track view->table
-- dependencies for implementing RESTRICT
DROP TABLE table1;

DROP VIEW view1;
DROP VIEW view2;
DROP VIEW view3;
DROP VIEW view4;
DROP VIEW view5;
DROP VIEW view6;
DROP VIEW view7;
DROP VIEW view8;
DROP VIEW view9;
DROP VIEW view10;
DROP VIEW view11;
DROP VIEW view12;
DROP VIEW view13;
DROP VIEW view14;
DROP TEMPORARY FUNCTION test_translate;
DROP TEMPORARY FUNCTION test_max;
DROP TEMPORARY FUNCTION test_explode;
