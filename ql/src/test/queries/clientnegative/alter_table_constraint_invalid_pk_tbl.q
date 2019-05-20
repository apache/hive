CREATE TABLE table1 (a STRING, b STRING, primary key (a) disable);
CREATE TABLE table2 (a STRING, b STRING);
alter table table3 add constraint pk3 primary key (a) disable novalidate rely;
