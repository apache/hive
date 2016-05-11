CREATE TABLE table1 (a STRING, b STRING, primary key (a) disable novalidate);
CREATE TABLE table2 (a STRING, b STRING, primary key (a) disable novalidate rely);
alter table table2 add constraint fk1 foreign key (b) references table3(a) disable novalidate;
