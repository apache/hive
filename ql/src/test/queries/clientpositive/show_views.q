--! qt:dataset::ONLY

CREATE DATABASE test1;
CREATE DATABASE test2;

USE test1;
CREATE TABLE shtb_test1_n1(KEY INT, VALUE STRING) PARTITIONED BY(ds STRING) STORED AS TEXTFILE;
CREATE VIEW shtb_test1_view1_n0 AS SELECT * FROM shtb_test1_n1 where KEY > 1000 and KEY < 2000;
CREATE VIEW shtb_test1_view2_n0 AS SELECT * FROM shtb_test1_n1 where KEY > 100 and KEY < 200;
CREATE VIEW shtb_full_view2_n0 AS SELECT * FROM shtb_test1_n1;
USE test2;
CREATE TABLE shtb_test1_n1(KEY INT, VALUE STRING) PARTITIONED BY(ds STRING) STORED AS TEXTFILE;
CREATE TABLE shtb_test2_n1(KEY INT, VALUE STRING) PARTITIONED BY(ds STRING) STORED AS TEXTFILE;
CREATE VIEW shtb_test1_view1_n0 AS SELECT * FROM shtb_test1_n1 where KEY > 1000 and KEY < 2000;
CREATE VIEW shtb_test2_view2_n0 AS SELECT * FROM shtb_test2_n1 where KEY > 100 and KEY < 200;

USE test1;
EXPLAIN SHOW VIEWS;
SHOW VIEWS;
EXPLAIN SHOW VIEWS 'test_%';
SHOW VIEWS 'test_%';
SHOW VIEWS '%view2';

USE test2;
SHOW VIEWS 'shtb_%';

-- SHOW VIEWS basic syntax tests
USE default;
EXPLAIN SHOW VIEWS FROM test1;
SHOW VIEWS FROM test1;
SHOW VIEWS FROM test2;
EXPLAIN SHOW VIEWS IN test1;
SHOW VIEWS IN test1;
SHOW VIEWS IN default;
EXPLAIN SHOW VIEWS IN test1 "shtb_test_%";
SHOW VIEWS IN test1 "shtb_test_%";
SHOW VIEWS IN test2 LIKE "nomatch";

-- SHOW VIEWS from a database with a name that requires escaping
CREATE DATABASE `database`;
USE `database`;
CREATE TABLE foo_n8(a INT);
CREATE VIEW fooview_n0 AS SELECT * FROM foo_n8;
USE default;
SHOW VIEWS FROM `database` LIKE "fooview_n0";

DROP VIEW fooview_n0;
DROP TABLE foo_n8;

USE test1;
DROP VIEW shtb_test1_view1_n0;
DROP VIEW shtb_test1_view2_n0;
DROP VIEW shtb_full_view2_n0;
DROP TABLE shtb_test1_n1;
DROP DATABASE test1;

USE test2;
DROP VIEW shtb_test1_view1_n0;
DROP VIEW shtb_test2_view2_n0;
DROP TABLE shtb_test1_n1;
DROP TABLE shtb_test2_n1;
DROP DATABASE test2;
