set hive.mapred.mode=nonstrict;
set hive.support.concurrency = true;

-- CREATE DATABASE in default catalog 'hive'
CREATE DATABASE testdb;

-- Check databases in default catalog 'hive',
-- The list of databases in the catalog 'hive' should only contain the default and the testdb.
SHOW DATABASES;

-- CREATE a new catalog with comment
CREATE CATALOG testcat LOCATION '/tmp/testcat' COMMENT 'Hive test catalog';

-- Check catalogs list
SHOW CATALOGS;

-- Switch the catalog from hive to 'testcat'
SET CATALOG testcat;

-- CREATE DATABASE in default catalog 'hive'
CREATE DATABASE testdb_new;

-- Check databases in catalog 'testcat',
-- The list of databases in the catalog 'hive' should only contain the default and the testdb_new.
SHOW DATABASES;

-- Switch database by catalog.db pattern
USE testcat.testdb_new;
