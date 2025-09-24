-- CREATE DATABASE in default catalog 'hive'
CREATE DATABASE testdb;

-- Check databases in default catalog 'hive',
-- The list of databases in the catalog 'hive' should only contain the default and the testdb.
SHOW DATABASES;

-- CREATE a new catalog with comment
CREATE CATALOG testcat LOCATION '/tmp/testcat' COMMENT 'Hive test catalog';

-- Check catalogs list
SHOW CATALOGS;

-- CREATE DATABASE in new catalog testcat by catalog.db pattern
CREATE DATABASE testcat.testdb_1;

-- Switch the catalog from hive to 'testcat'
SET CATALOG testcat;

-- CREATE DATABASE in new catalog testcat
CREATE DATABASE testdb_2;

-- Check databases in catalog 'testcat',
-- The list of databases in the catalog 'hive' should contain default and testdb_1 and testdb_2.
SHOW DATABASES;

-- Switch database by catalog.db pattern
USE testcat.testdb_1;

-- Drop database by catalog.db pattern
DROP DATABASE testcat.testdb_1;

-- Check databases in catalog 'testcat',
-- The list of databases in the catalog 'hive' should contain default and testdb_2.
SHOW DATABASES;
