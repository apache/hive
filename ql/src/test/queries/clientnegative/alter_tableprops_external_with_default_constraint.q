CREATE TABLE table1 (a STRING DEFAULT 'hive', b STRING);
Alter table table1 set TBLPROPERTIES('external'='true');

