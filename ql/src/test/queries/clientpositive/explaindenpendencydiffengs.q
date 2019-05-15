drop table if exists dependtest;
create table dependtest (a string) partitioned by (b int);
insert into table dependtest partition (b=1) values ("hello");
create view viewtest as select * from dependtest where b = 1;
set hive.cbo.enable=false;
set hive.compute.query.using.stats=false;
explain dependency select count(*) from viewtest ;
drop view viewtest;
drop table dependtest;

