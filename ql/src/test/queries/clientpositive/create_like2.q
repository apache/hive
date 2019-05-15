-- Tests the copying over of Table Parameters according to a HiveConf setting
-- when doing a CREATE TABLE LIKE.

CREATE TABLE table1_n20(a INT, b STRING);
ALTER TABLE table1_n20 SET TBLPROPERTIES ('a'='1', 'b'='2', 'c'='3', 'd' = '4');

SET hive.ddl.createtablelike.properties.whitelist=a,c,D;
CREATE TABLE table2_n14 LIKE table1_n20;
DESC FORMATTED table2_n14;
