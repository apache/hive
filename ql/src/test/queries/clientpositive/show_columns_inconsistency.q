CREATE DATABASE IF NOT EXISTS col_test_db;
USE col_test_db;

CREATE TABLE wildcard_table (
  id_primary INT,
  id_secondary INT,
  name_first STRING,
  name_last STRING
);


SHOW TABLES LIKE 'wild%';
SHOW TABLES LIKE 'wild*';
SHOW TABLES LIKE 'none|wildcard_table';

SHOW COLUMNS FROM wildcard_table LIKE 'id%';
SHOW COLUMNS FROM wildcard_table LIKE 'name_%';
SHOW COLUMNS FROM wildcard_table LIKE 'id*';
SHOW COLUMNS FROM wildcard_table LIKE 'id_primary|name_first';

DROP DATABASE col_test_db CASCADE;
