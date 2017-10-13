CREATE TABLE mm_table(key int, value string) stored as rcfile tblproperties ("transactional"="true", "transactional_properties"="insert_only");

TRUNCATE TABLE mm_table COLUMNS (value);
