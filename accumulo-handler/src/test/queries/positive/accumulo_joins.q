DROP TABLE users;
DROP TABLE states;
DROP TABLE countries;
DROP TABLE users_level;

-- From HIVE-1257

CREATE TABLE users(key string, state string, country string, country_id int)
STORED BY 'org.apache.hadoop.hive.accumulo.AccumuloStorageHandler'
WITH SERDEPROPERTIES (
"accumulo.columns.mapping" = ":rowID,info:state,info:country,info:country_id"
);

CREATE TABLE states(key string, name string)
STORED BY 'org.apache.hadoop.hive.accumulo.AccumuloStorageHandler'
WITH SERDEPROPERTIES (
"accumulo.columns.mapping" = ":rowID,state:name"
);

CREATE TABLE countries(key string, name string, country string, country_id int)
STORED BY 'org.apache.hadoop.hive.accumulo.AccumuloStorageHandler'
WITH SERDEPROPERTIES (
"accumulo.columns.mapping" = ":rowID,info:name,info:country,info:country_id"
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

CREATE TABLE users(key int, userid int, username string, created int) 
STORED BY 'org.apache.hadoop.hive.accumulo.AccumuloStorageHandler'
WITH SERDEPROPERTIES ("accumulo.columns.mapping" = ":rowID,f:userid,f:nickname,f:created");

CREATE TABLE users_level(key int, userid int, level int)
STORED BY 'org.apache.hadoop.hive.accumulo.AccumuloStorageHandler'
WITH SERDEPROPERTIES ("accumulo.columns.mapping" = ":rowID,f:userid,f:level");

-- HIVE-1903:  the problem fixed here showed up even without any data,
-- so no need to load any to test it
SELECT year(from_unixtime(users.created)) AS year, level, count(users.userid) AS num 
 FROM users JOIN users_level ON (users.userid = users_level.userid) 
 GROUP BY year(from_unixtime(users.created)), level;

DROP TABLE users;
DROP TABLE users_level;
