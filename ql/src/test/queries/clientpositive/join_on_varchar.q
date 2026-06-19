--! qt:dataset:src
set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS

create table tbl1_n3(c1 varchar(10), intcol int);
create table tbl2_n2(c2 varchar(30));
insert into table tbl1_n3 select repeat('t', 10), 11 from src limit 1;
insert into table tbl1_n3 select repeat('s', 10), 22 from src limit 1;
insert into table tbl2_n2 select concat(repeat('t', 10), 'ppp') from src limit 1;
insert into table tbl2_n2 select repeat('s', 10) from src limit 1;
set hive.auto.convert.join=true;

explain
select /*+ MAPJOIN(tbl2_n2) */ c1,c2 from tbl1_n3 join tbl2_n2 on (c1 = c2) order by c1,c2;
select /*+ MAPJOIN(tbl2_n2) */ c1,c2 from tbl1_n3 join tbl2_n2 on (c1 = c2) order by c1,c2;
