--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.optimize.cte.materialize.threshold=-1;
set hive.explain.user=true;

create database mydb;
use mydb;
create table q1_n0 (colnum int, colstring string);
insert into q1_n0 values (5, 'A');

use default;

show tables in mydb;

explain
with q1_n0 as (select * from src where key= '5')
select a.colnum
from mydb.q1_n0 as a join q1_n0 as b
on a.colnum=b.key;

with q1_n0 as (select * from src where key= '5')
select a.colnum
from mydb.q1_n0 as a join q1_n0 as b
on a.colnum=b.key;
