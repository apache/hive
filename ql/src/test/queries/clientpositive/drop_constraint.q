drop table table2;
CREATE TABLE table2 (a STRING, b STRING, constraint pk1 primary key (a) disable);
ALTER TABLE table2 DROP CONSTRAINT if exists pk1;
ALTER TABLE table2 DROP CONSTRAINT if exists pk1;
ALTER TABLE table2 DROP CONSTRAINT if exists pk2;
drop table table2;
