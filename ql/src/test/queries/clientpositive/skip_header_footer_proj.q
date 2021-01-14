SET hive.fetch.task.conversion=none;
-- no footer
DROP TABLE IF EXISTS hf1;
CREATE TABLE hf1 (a string, b string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' TBLPROPERTIES('skip.header.line.count'='1');
INSERT OVERWRITE TABLE hf1 VALUES ('x','y'),('a','b'),('c','d');
SELECT * FROM hf1;

-- no header
DROP TABLE IF EXISTS hf2;
CREATE TABLE hf2 (a string, b string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' TBLPROPERTIES('skip.footer.line.count'='2');
INSERT OVERWRITE TABLE hf2 VALUES ('x','y'),('a','b'),('c','d');
SELECT * FROM hf2;

-- only header, no data
DROP TABLE IF EXISTS hf3;
CREATE TABLE hf3 (a string, b string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' TBLPROPERTIES('skip.header.line.count'='3');
INSERT OVERWRITE TABLE hf3 VALUES ('x','y'),('a','b'),('c','d');
SELECT * FROM hf3;

-- header and footer, no data
DROP TABLE IF EXISTS hf4;
CREATE TABLE hf4 (a string, b string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' TBLPROPERTIES('skip.header.line.count'='1','skip.footer.line.count'='2');
INSERT OVERWRITE TABLE hf4 VALUES ('x','y'),('a','b'),('c','d');
SELECT * FROM hf4;

