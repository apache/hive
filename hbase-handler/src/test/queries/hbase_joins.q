DROP TABLE users;
DROP TABLE states;
DROP TABLE countries;

-- From HIVE-1257

CREATE TABLE users(key string, state string, country string, country_id int)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
"hbase.columns.mapping" = "info:state,info:country,info:country_id"
);

CREATE TABLE states(key string, name string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
"hbase.columns.mapping" = "state:name"
);

CREATE TABLE countries(key string, name string, country string, country_id int)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
"hbase.columns.mapping" = "info:name,info:country,info:country_id"
);

INSERT OVERWRITE TABLE users SELECT 'user1', 'IA', 'USA', 0
FROM src WHERE key=100;

INSERT OVERWRITE TABLE states SELECT 'IA', 'Iowa'
FROM src WHERE key=100;

INSERT OVERWRITE TABLE countries SELECT 'USA', 'United States', 'USA', 1
FROM src WHERE key=100;

set hive.input.format = org.apache.hadoop.hive.ql.io.HiveInputFormat;

SELECT u.key, u.country, c.name, c.key FROM users u JOIN countries c 
ON (u.country = c.key);

SELECT u.key, u.country, c.name, c.key FROM users u JOIN countries c
ON (u.country = c.country);

SELECT u.key, u.country, c.name, c.key FROM users u JOIN countries c 
ON (u.country_id = c.country_id);

SELECT u.key, u.state, s.name FROM users u JOIN states s 
ON (u.state = s.key);

set hive.input.format = org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

SELECT u.key, u.country, c.name, c.key FROM users u JOIN countries c 
ON (u.country = c.key);

SELECT u.key, u.country, c.name, c.key FROM users u JOIN countries c
ON (u.country = c.country);

SELECT u.key, u.country, c.name, c.key FROM users u JOIN countries c 
ON (u.country_id = c.country_id);

SELECT u.key, u.state, s.name FROM users u JOIN states s 
ON (u.state = s.key);

DROP TABLE users;
DROP TABLE states;
DROP TABLE countries;
