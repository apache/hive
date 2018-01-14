set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;
set hive.materializedview.rewriting=true;

CREATE DATABASE test1;
CREATE DATABASE test2;

USE test1;
CREATE TABLE shtb_test1(KEY INT, VALUE STRING) PARTITIONED BY(ds STRING)
STORED AS ORC TBLPROPERTIES ('transactional'='true');
CREATE MATERIALIZED VIEW shtb_test1_view1 AS
SELECT * FROM shtb_test1 where KEY > 1000 and KEY < 2000;
CREATE MATERIALIZED VIEW shtb_test1_view2 ENABLE REWRITE AS
SELECT * FROM shtb_test1 where KEY > 100 and KEY < 200;
CREATE MATERIALIZED VIEW shtb_full_view2 ENABLE REWRITE AS
SELECT * FROM shtb_test1;

USE test2;
CREATE TABLE shtb_test1(KEY INT, VALUE STRING) PARTITIONED BY(ds STRING)
STORED AS TEXTFILE;
CREATE TABLE shtb_test2(KEY INT, VALUE STRING) PARTITIONED BY(ds STRING)
STORED AS TEXTFILE;
CREATE MATERIALIZED VIEW shtb_test1_view1 AS
SELECT * FROM shtb_test1 where KEY > 1000 and KEY < 2000;
CREATE MATERIALIZED VIEW shtb_test2_view2 AS
SELECT * FROM shtb_test2 where KEY > 100 and KEY < 200;

USE test1;
SHOW MATERIALIZED VIEWS;
SHOW MATERIALIZED VIEWS 'test_*';
SHOW MATERIALIZED VIEWS '*view2';
SHOW MATERIALIZED VIEWS LIKE 'test_view1|test_view2';

USE test2;
SHOW MATERIALIZED VIEWS 'shtb_*';

-- SHOW MATERIALIZED VIEWS basic syntax tests
USE default;
SHOW MATERIALIZED VIEWS FROM test1;
SHOW MATERIALIZED VIEWS FROM test2;
SHOW MATERIALIZED VIEWS IN test1;
SHOW MATERIALIZED VIEWS IN default;
SHOW MATERIALIZED VIEWS IN test1 "shtb_test_*";
SHOW MATERIALIZED VIEWS IN test2 LIKE "nomatch";

-- SHOW MATERIALIZED VIEWS from a database with a name that requires escaping
CREATE DATABASE `database`;
USE `database`;
CREATE TABLE foo(a INT);
CREATE VIEW fooview AS
SELECT * FROM foo;
USE default;
SHOW MATERIALIZED VIEWS FROM `database` LIKE "fooview";

DROP MATERIALIZED VIEW fooview;
DROP TABLE foo;

USE test1;
DROP MATERIALIZED VIEW shtb_test1_view1;
DROP MATERIALIZED VIEW shtb_test1_view2;
DROP MATERIALIZED VIEW shtb_full_view2;
DROP TABLE shtb_test1;
DROP DATABASE test1;

USE test2;
DROP MATERIALIZED VIEW shtb_test1_view1;
DROP MATERIALIZED VIEW shtb_test2_view2;
DROP TABLE shtb_test1;
DROP TABLE shtb_test2;
DROP DATABASE test2;
