CREATE TABLE table1 (a STRING, b STRING, primary key (a) disable novalidate);
CREATE TABLE table2 (a STRING, b STRING, constraint pk1 primary key (a) disable novalidate);
CREATE TABLE table3 (x string, PRIMARY KEY (x) disable novalidate, CONSTRAINT fk1 FOREIGN KEY (x) REFERENCES table2(b)  DISABLE NOVALIDATE); 
CREATE TABLE table4 (x string, y string, PRIMARY KEY (x) disable novalidate, CONSTRAINT fk2 FOREIGN KEY (x) REFERENCES table2(b)  DISABLE NOVALIDATE, 
CONSTRAINT fk3 FOREIGN KEY (y) REFERENCES table2(a)  DISABLE NOVALIDATE);
CREATE TABLE table5 (x string, PRIMARY KEY (x) disable novalidate, FOREIGN KEY (x) REFERENCES table2(b)  DISABLE NOVALIDATE);
CREATE TABLE table6 (x string, y string, PRIMARY KEY (x) disable novalidate, FOREIGN KEY (x) REFERENCES table2(b)  DISABLE NOVALIDATE,
CONSTRAINT fk4 FOREIGN KEY (y) REFERENCES table1(a)  DISABLE NOVALIDATE);
CREATE TABLE table7 (a STRING, b STRING, primary key (a) disable novalidate rely);
CREATE TABLE table8 (a STRING, b STRING, constraint pk8 primary key (a) disable novalidate norely);


