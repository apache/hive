set hive.mapred.mode=nonstrict;
set hive.support.concurrency = true;
--! qt:replace:/\d{4}-\d{2}-\d{2}.*/#Masked#/

dfs -mkdir -p hdfs:///tmp/test_cat;

-- SORT_QUERY_RESULTS
SHOW CATALOGS;

-- CREATE with comment and default location
CREATE CATALOG test_cat COMMENT 'Hive test catalog' PROPERTIES('type'='NATIVE');

-- DESCRIBE
DESC CATALOG test_cat;

-- CREATE INE already exists
CREATE CATALOG IF NOT EXISTS test_cat LOCATION 'hdfs:///tmp/test_cat' PROPERTIES('type'='native');
SHOW CATALOGS;

-- DROP
DROP CATALOG test_cat;
SHOW CATALOGS;

-- CREATE INE doesn't exist
CREATE CATALOG IF NOT EXISTS test_cat LOCATION 'hdfs:///tmp/test_cat' COMMENT 'Hive test catalog' PROPERTIES('type'='native');
SHOW CATALOGS;

-- DROP IE exists
DROP CATALOG IF EXISTS test_cat;
SHOW CATALOGS;

-- DROP IE doesn't exist
DROP CATALOG IF EXISTS test_cat;

-- SHOW
CREATE CATALOG test_cat LOCATION 'hdfs:///tmp/test_cat' COMMENT 'Hive test catalog' PROPERTIES('type'='native');
SHOW CATALOGS;

-- SHOW pattern
SHOW CATALOGS LIKE 'test%';

-- SHOW pattern
SHOW CATALOGS LIKE 'test_';

-- SHOW pattern
SHOW CATALOGS LIKE 'test__';

-- ALTER LOCATION
ALTER CATALOG test_cat SET LOCATION 'hdfs:///tmp/test_cat_new';
DESC CATALOG EXTENDED test_cat;

-- ALTER PROPERTIES.
-- TODO catalog. Check the catalog's properties after we implement 'desc formatted' or 'show create catalog'.
ALTER CATALOG test_cat SET PROPERTIES ('key2'='value2');

-- DROP catalog at the end
DROP CATALOG test_cat;
