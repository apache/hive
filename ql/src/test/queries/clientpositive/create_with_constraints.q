CREATE TABLE table1 (a STRING, b STRING, primary key (a) disable novalidate);
CREATE TABLE table2 (a STRING, b STRING, constraint pk1 primary key (a) disable novalidate);
CREATE TABLE table3 (x string, PRIMARY KEY (x) disable novalidate, CONSTRAINT fk1 FOREIGN KEY (x) REFERENCES table2(a)  DISABLE NOVALIDATE); 
CREATE TABLE table4 (x string, y string, PRIMARY KEY (x) disable novalidate, CONSTRAINT fk2 FOREIGN KEY (x) REFERENCES table2(a)  DISABLE NOVALIDATE, 
CONSTRAINT fk3 FOREIGN KEY (y) REFERENCES table2(a)  DISABLE NOVALIDATE);
CREATE TABLE table5 (x string, PRIMARY KEY (x) disable novalidate, FOREIGN KEY (x) REFERENCES table2(a)  DISABLE NOVALIDATE);
CREATE TABLE table6 (x string, y string, PRIMARY KEY (x) disable novalidate, FOREIGN KEY (x) REFERENCES table2(a)  DISABLE NOVALIDATE,
CONSTRAINT fk4 FOREIGN KEY (y) REFERENCES table1(a)  DISABLE NOVALIDATE);
CREATE TABLE table7 (a STRING, b STRING, primary key (a) disable novalidate rely);
CREATE TABLE table8 (a STRING, b STRING, constraint pk8 primary key (a) disable novalidate norely);
CREATE TABLE table9 (a STRING, b STRING, primary key (a, b) disable novalidate rely);
CREATE TABLE table10 (a STRING, b STRING, constraint pk10 primary key (a) disable novalidate norely, foreign key (a, b) references table9(a, b) disable novalidate);
CREATE TABLE table11 (a STRING, b STRING, c STRING, constraint pk11 primary key (a) disable novalidate rely, constraint fk11_1 foreign key (a, b) references table9(a, b) disable novalidate,
constraint fk11_2 foreign key (c) references table4(x) disable novalidate);

DESCRIBE EXTENDED table1;
DESCRIBE EXTENDED table2;
DESCRIBE EXTENDED table3;
DESCRIBE EXTENDED table4;
DESCRIBE EXTENDED table5;
DESCRIBE EXTENDED table6;
DESCRIBE EXTENDED table7;
DESCRIBE EXTENDED table8;
DESCRIBE EXTENDED table9;
DESCRIBE EXTENDED table10;
DESCRIBE EXTENDED table11;

DESCRIBE FORMATTED table1;
DESCRIBE FORMATTED table2;
DESCRIBE FORMATTED table3;
DESCRIBE FORMATTED table4;
DESCRIBE FORMATTED table5;
DESCRIBE FORMATTED table6;
DESCRIBE FORMATTED table7;
DESCRIBE FORMATTED table8;
DESCRIBE FORMATTED table9;
DESCRIBE FORMATTED table10;
DESCRIBE FORMATTED table11;

ALTER TABLE table2 DROP CONSTRAINT pk1;
ALTER TABLE table3 DROP CONSTRAINT fk1;
ALTER TABLE table6 DROP CONSTRAINT fk4;

DESCRIBE EXTENDED table2;
DESCRIBE EXTENDED table3;
DESCRIBE EXTENDED table6;

DESCRIBE FORMATTED table2;
DESCRIBE FORMATTED table3;
DESCRIBE FORMATTED table6;

ALTER TABLE table2 ADD CONSTRAINT pkt2 primary key (a) disable novalidate;
ALTER TABLE table3 ADD CONSTRAINT fk1 FOREIGN KEY (x) REFERENCES table2(a)  DISABLE NOVALIDATE RELY;
ALTER TABLE table6 ADD CONSTRAINT fk4 FOREIGN KEY (y) REFERENCES table1(a)  DISABLE NOVALIDATE;

DESCRIBE FORMATTED table2;
DESCRIBE FORMATTED table3;
DESCRIBE FORMATTED table6;

CREATE DATABASE dbconstraint;
USE dbconstraint;
CREATE TABLE table2 (a STRING, b STRING, constraint pk1 primary key (a) disable novalidate);
USE default;

DESCRIBE EXTENDED dbconstraint.table2;
DESCRIBE FORMATTED dbconstraint.table2;

ALTER TABLE dbconstraint.table2 DROP CONSTRAINT pk1;

DESCRIBE EXTENDED dbconstraint.table2;
DESCRIBE FORMATTED dbconstraint.table2;

ALTER TABLE dbconstraint.table2 ADD CONSTRAINT pk1 primary key (a) disable novalidate;
DESCRIBE FORMATTED dbconstraint.table2;
ALTER TABLE dbconstraint.table2  ADD CONSTRAINT fkx FOREIGN KEY (b) REFERENCES table1(a)  DISABLE NOVALIDATE;
DESCRIBE FORMATTED dbconstraint.table2;
