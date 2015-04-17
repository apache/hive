-- SORT_QUERY_RESULTS

create table tbl1(c1 varchar(10), intcol int);
create table tbl2(c2 varchar(30));
insert into table tbl1 select repeat('t', 10), 11 from src limit 1;
insert into table tbl1 select repeat('s', 10), 22 from src limit 1;
insert into table tbl2 select concat(repeat('t', 10), 'ppp') from src limit 1;
insert into table tbl2 select repeat('s', 10) from src limit 1;

explain
select /*+ MAPJOIN(tbl2) */ c1,c2 from tbl1 join tbl2 on (c1 = c2) order by c1,c2;
select /*+ MAPJOIN(tbl2) */ c1,c2 from tbl1 join tbl2 on (c1 = c2) order by c1,c2;
