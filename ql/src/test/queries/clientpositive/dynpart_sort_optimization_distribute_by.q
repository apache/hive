set hive.exec.dynamic.partition.mode=nonstrict;
set hive.optimize.sort.dynamic.partition=true;
set hive.vectorized.execution.enabled=false;


create table table1 (col1 string, datekey int);
insert into table1 values ('ROW1', 1), ('ROW2', 2), ('ROW3', 1);
create table table2 (col1 string) partitioned by (datekey int);

explain extended insert into table table2
PARTITION(datekey)
select col1,
datekey
from table1
distribute by datekey;


insert into table table2
PARTITION(datekey)
select col1,
datekey
from table1
distribute by datekey;

select * from table1;
select * from table2;