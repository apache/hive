SET hive.metastore.disallow.incompatible.col.type.changes=false;
SELECT * FROM src LIMIT 1;
CREATE TABLE test_table123 (a INT, b MAP<STRING, STRING>) PARTITIONED BY (ds STRING) STORED AS SEQUENCEFILE;
INSERT OVERWRITE TABLE test_table123 PARTITION(ds="foo1") SELECT 1, MAP("a1", "b1") FROM src LIMIT 1;
SELECT * from test_table123 WHERE ds="foo1";
ALTER TABLE test_table123 REPLACE COLUMNS (a INT, b MAP<STRING, STRING>);
ALTER TABLE test_table123 REPLACE COLUMNS (a BIGINT, b MAP<STRING, STRING>);
ALTER TABLE test_table123 REPLACE COLUMNS (a INT, b MAP<STRING, STRING>);
ALTER TABLE test_table123 REPLACE COLUMNS (a DOUBLE, b MAP<STRING, STRING>);
ALTER TABLE test_table123 REPLACE COLUMNS (a TINYINT, b MAP<STRING, STRING>);
ALTER TABLE test_table123 REPLACE COLUMNS (a BOOLEAN, b MAP<STRING, STRING>);
ALTER TABLE test_table123 REPLACE COLUMNS (a TINYINT, b MAP<STRING, STRING>);
ALTER TABLE test_table123 CHANGE COLUMN a a_new BOOLEAN;

SET hive.metastore.disallow.incompatible.col.type.changes=true;
-- All the above ALTERs will succeed since they are between compatible types.
-- The following ALTER will fail as MAP<STRING, STRING> and STRING are not
-- compatible.

ALTER TABLE test_table123 REPLACE COLUMNS (a INT, b STRING);
reset hive.metastore.disallow.incompatible.col.type.changes;
