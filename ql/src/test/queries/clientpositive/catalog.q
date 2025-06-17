set hive.mapred.mode=nonstrict;
set hive.support.concurrency = true;

-- SORT_QUERY_RESULTS
SHOW CATALOGS;

-- CREATE with comment
CREATE CATALOG test_cat LOCATION '/tmp/test_cat' COMMENT 'Hive test catalog';

-- DESCRIBE
DESC CATALOG test_cat;

-- CREATE INE already exists
CREATE CATALOG IF NOT EXISTS test_cat LOCATION '/tmp/test_cat';
SHOW CATALOGS;

-- DROP
DROP CATALOG test_cat;
SHOW CATALOGS;

-- CREATE INE doesn't exist
CREATE CATALOG IF NOT EXISTS test_cat LOCATION '/tmp/test_cat' COMMENT 'Hive test catalog';
SHOW CATALOGS;

-- DROP IE exists
DROP CATALOG IF EXISTS test_cat;
SHOW CATALOGS;

-- DROP IE doesn't exist
DROP CATALOG IF EXISTS test_cat;

-- SHOW
CREATE CATALOG test_cat LOCATION '/tmp/test_cat' COMMENT 'Hive test catalog';
SHOW CATALOGS;

-- SHOW pattern
SHOW CATALOGS LIKE 'test%';

-- SHOW pattern
SHOW CATALOGS LIKE 'test_';

-- SHOW pattern
SHOW CATALOGS LIKE 'test__';

-- ALTER LOCATION
ALTER CATALOG test_cat SET LOCATION '/tmp/test_cat_new';
DESC CATALOG EXTENDED test_cat;
