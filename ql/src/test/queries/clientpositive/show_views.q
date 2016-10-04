CREATE DATABASE test1;
CREATE DATABASE test2;

USE test1;
CREATE TABLE shtb_test1(KEY INT, VALUE STRING) PARTITIONED BY(ds STRING) STORED AS TEXTFILE;
CREATE VIEW shtb_test1_view1 AS SELECT * FROM shtb_test1 where KEY > 1000 and KEY < 2000;
CREATE VIEW shtb_test1_view2 AS SELECT * FROM shtb_test1 where KEY > 100 and KEY < 200;
CREATE VIEW shtb_full_view2 AS SELECT * FROM shtb_test1;
USE test2;
CREATE TABLE shtb_test1(KEY INT, VALUE STRING) PARTITIONED BY(ds STRING) STORED AS TEXTFILE;
CREATE TABLE shtb_test2(KEY INT, VALUE STRING) PARTITIONED BY(ds STRING) STORED AS TEXTFILE;
CREATE VIEW shtb_test1_view1 AS SELECT * FROM shtb_test1 where KEY > 1000 and KEY < 2000;
CREATE VIEW shtb_test2_view2 AS SELECT * FROM shtb_test2 where KEY > 100 and KEY < 200;

USE test1;
SHOW VIEWS;
SHOW VIEWS 'test_*';
SHOW VIEWS '*view2';
SHOW VIEWS LIKE 'test_view1|test_view2';

USE test2;
SHOW VIEWS 'shtb_*';

-- SHOW VIEWS basic syntax tests
USE default;
SHOW VIEWS FROM test1;
SHOW VIEWS FROM test2;
SHOW VIEWS IN test1;
SHOW VIEWS IN default;
SHOW VIEWS IN test1 "shtb_test_*";
SHOW VIEWS IN test2 LIKE "nomatch";

-- SHOW VIEWS from a database with a name that requires escaping
CREATE DATABASE `database`;
USE `database`;
CREATE TABLE foo(a INT);
CREATE VIEW fooview AS SELECT * FROM foo;
USE default;
SHOW VIEWS FROM `database` LIKE "fooview";

DROP VIEW fooview;
DROP TABLE foo;

USE test1;
DROP VIEW shtb_test1_view1;
DROP VIEW shtb_test1_view2;
DROP VIEW shtb_full_view2;
DROP TABLE shtb_test1;
DROP DATABASE test1;

USE test2;
DROP VIEW shtb_test1_view1;
DROP VIEW shtb_test2_view2;
DROP TABLE shtb_test1;
DROP TABLE shtb_test2;
DROP DATABASE test2;
