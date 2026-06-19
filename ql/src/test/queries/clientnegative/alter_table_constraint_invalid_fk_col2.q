CREATE TABLE table1 (a STRING, b STRING, primary key (a) disable);
CREATE TABLE table2 (a STRING, b STRING, primary key (a) disable rely);
alter table table2 add constraint fk1 foreign key (b) references table1(c) disable novalidate;
