CREATE TABLE table2 (a STRING, b STRING, constraint pk1 primary key (a) disable);
ALTER TABLE table1 DROP CONSTRAINT pk1;
