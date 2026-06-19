set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
set hive.security.authorization.enabled=true;

-- Drop table command for non-existing DB

DROP TABLE auth_db.auth_permanent_table;

-- Drop table command for non-existing DB

DROP TABLE IF EXISTS auth_db.auth_permanent_table;

-- Drop non-existing table with DB Drop Privileges

set hive.security.authorization.enabled=false;

CREATE DATABASE auth_db;
GRANT DROP ON DATABASE auth_db TO USER hive_test_user;

set hive.security.authorization.enabled=true;

DROP TABLE auth_db.auth_permanent_table;

-- Drop non-existing table with IF EXISTS clause with DB Drop Privileges

DROP TABLE IF EXISTS auth_db.auth_permanent_table;

--- Create tables for test

set hive.security.authorization.enabled=false;

create table auth_db.drop_table_auth_1 (key int, value string) partitioned by (ds string);
create table auth_db.drop_table_auth_2 (key int, value string);
CREATE TEMPORARY TABLE auth_temp_table_1(key STRING, c1 INT, c2 STRING) STORED AS TEXTFILE;
CREATE TEMPORARY TABLE auth_temp_table_2(key STRING, c1 INT, c2 STRING) STORED AS TEXTFILE;

-- Drop existing regular table

set hive.security.authorization.enabled=true;
DROP TABLE auth_db.drop_table_auth_1;

-- Drop existing regular table with IF EXISTS

DROP TABLE IF EXISTS auth_db.drop_table_auth_2;

-- Drop temporary table

DROP TABLE auth_db.auth_temp_table_1;

-- Drop temporary table with IF EXISTS

DROP TABLE IF EXISTS auth_db.auth_temp_table_2;


-- Drop non-existing table from current database

set hive.security.authorization.enabled=false;

CREATE DATABASE auth_db_1;
use auth_db_1;
GRANT DROP ON DATABASE auth_db_1 TO USER hive_test_user;

create table drop_table_auth_3 (key int, value string) partitioned by (ds string);
create table drop_table_auth_4 (key int, value string);
CREATE TEMPORARY TABLE auth_temp_table_1(key STRING, c1 INT, c2 STRING) STORED AS TEXTFILE;
CREATE TEMPORARY TABLE auth_temp_table_2(key STRING, c1 INT, c2 STRING) STORED AS TEXTFILE;

set hive.security.authorization.enabled=true;

DROP TABLE auth_temp_table;

-- Drop non-existing table with IF EXISTS from current database

DROP TABLE IF EXISTS auth_temp_table;

-- Drop existing regular table from current database

DROP TABLE drop_table_auth_3;

-- Drop existing regular table with IF EXISTS from current database

DROP TABLE IF EXISTS drop_table_auth_4;

-- Drop temporary table from current database

DROP TABLE auth_temp_table_1;

-- Drop temporary table with IF EXISTS from current database

DROP TABLE IF EXISTS auth_temp_table_2;
