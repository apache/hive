create table testpart1(id int) partitioned by (dept string);
alter table testpart1 add partition(dept='a');
insert into table testpart1 partition(dept='a') values (1);
analyze table testpart1 partition(dept='a') compute statistics for columns;
alter table testpart1 rename to testpart1_rename;
drop table testpart1_rename;