CREATE TABLE table1 (a STRING, b STRING, primary key (a) disable);
alter table table1 add constraint pk4 primary key (b) disable novalidate rely;
