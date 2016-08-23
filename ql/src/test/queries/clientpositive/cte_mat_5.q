set hive.mapred.mode=nonstrict;
set hive.optimize.cte.materialize.threshold=1;
set hive.explain.user=true;

create database mydb;
use mydb;
create table q1 (colnum int, colstring string);
insert into q1 values (5, 'A');

use default;

show tables in mydb;
show tables;

explain
with q1 as (select * from src where key= '5')
select a.colnum
from mydb.q1 as a join q1 as b
on a.colnum=b.key;

with q1 as (select * from src where key= '5')
select a.colnum
from mydb.q1 as a join q1 as b
on a.colnum=b.key;
