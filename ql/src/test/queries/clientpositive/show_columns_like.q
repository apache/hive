CREATE DATABASE IF NOT EXISTS col_test_db;
USE col_test_db;

CREATE TABLE wildcard_table (
  id_primary INT,
  id_secondary INT,
  name_first STRING,
  name_last STRING,
  MixedCaseColumn INT,
  another_Mixed_Col STRING
);

SHOW COLUMNS FROM wildcard_table LIKE 'id%';
SHOW COLUMNS FROM wildcard_table LIKE 'name_%';
-- Case Insensitivity test
SHOW COLUMNS FROM wildcard_table LIKE 'mixedcase%';
SHOW COLUMNS FROM wildcard_table LIKE 'another_mixed_col';
SHOW COLUMNS FROM wildcard_table LIKE 'id*';
SHOW COLUMNS FROM wildcard_table LIKE 'id_primary|name_first';

-- Additional tests for '_' and empty results
SHOW COLUMNS FROM wildcard_table LIKE 'id_secondar_';
SHOW COLUMNS FROM wildcard_table LIKE 'id__rimary';
SHOW COLUMNS FROM wildcard_table LIKE 'abc%';
SHOW COLUMNS FROM wildcard_table LIKE 'id__';

DROP DATABASE col_test_db CASCADE;
