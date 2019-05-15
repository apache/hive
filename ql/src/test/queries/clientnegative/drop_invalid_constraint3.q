CREATE TABLE table2 (a STRING, b STRING, constraint pk1 primary key (a) disable);
ALTER TABLE table2 DROP CONSTRAINT pk2;
