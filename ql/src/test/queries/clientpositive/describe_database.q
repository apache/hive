CREATE DATABASE test_db WITH dbproperties ('key1' = 'value1', 'key2' = 'value2');

EXPLAIN DESC DATABASE test_db;
DESC DATABASE test_db;

EXPLAIN DESC DATABASE EXTENDED test_db;
DESC DATABASE EXTENDED test_db;
DESC SCHEMA EXTENDED test_db;

DROP DATABASE test_db;
