CREATE TABLE mm_table(key int, value string)  tblproperties('hivecommit'='true');

TRUNCATE TABLE mm_table COLUMNS (value);
