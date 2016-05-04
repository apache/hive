CREATE TABLE table2 (a STRING, b STRING, constraint pk1 primary key (a) disable novalidate);
ALTER TABLE table2 DROP CONSTRAINT pk2;
