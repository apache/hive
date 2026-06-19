
drop table if exists testctas1;
drop table if exists testctas2;
create table testctas1 (id int);
create table testctas2 as select * from testctas1 where 1=2;
