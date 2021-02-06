--SORT_QUERY_RESULTS

set hive.explain.user=false;
-- partitioned table
create table daysales (customer int) partitioned by (dt string);

insert into daysales partition(dt='2001-01-01') values(1);
insert into daysales partition(dt='2001-01-03') values(1);

explain prepare pPart1 from select count(*) from daysales where dt=? and customer=?;
prepare pPart1 from select count(*) from daysales where dt=? and customer=?;

explain extended execute pPart1 using '2001-01-01',1;
execute pPart1 using '2001-01-01',1;

