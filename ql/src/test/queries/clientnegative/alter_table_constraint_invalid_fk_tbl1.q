CREATE TABLE table1 (a STRING, b STRING, primary key (a) disable novalidate);
CREATE TABLE table2 (a STRING, b STRING, primary key (a) disable novalidate rely);
alter table table3 add constraint fk1 foreign key (c) references table1(a) disable novalidate;
