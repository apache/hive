--! qt:dataset:src
explain
select distinct key, "" as dummy1, "" as dummy2 from src tablesample (10 rows) order by key;

select distinct key, "" as dummy1, "" as dummy2 from src tablesample (10 rows) order by key;

explain
create table dummy_n6 as
select distinct key, "X" as dummy1, "X" as dummy2 from src tablesample (10 rows) order by key;

create table dummy_n6 as
select distinct key, "X" as dummy1, "X" as dummy2 from src tablesample (10 rows) order by key;

select key,dummy1,dummy2 from dummy_n6 order by key;

explain
select max('pants'), max('pANTS') from src group by key order by key limit 1;
select max('pants'), max('pANTS') from src group by key order by key limit 1;
