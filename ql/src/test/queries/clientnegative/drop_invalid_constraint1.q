CREATE TABLE table1 (a STRING, b STRING, constraint pk1 primary key (a) disable novalidate);
ALTER TABLE table1 DROP CONSTRAINT pk1;
ALTER TABLE table1 DROP CONSTRAINT pk1;
