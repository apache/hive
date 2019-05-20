--! qt:dataset:src
explain
select distinct key, "" as dummy1, "" as dummy2 from src tablesample (10 rows);

select distinct key, "" as dummy1, "" as dummy2 from src tablesample (10 rows);

explain
create table dummy_n6 as
select distinct key, "X" as dummy1, "X" as dummy2 from src tablesample (10 rows);

create table dummy_n6 as
select distinct key, "X" as dummy1, "X" as dummy2 from src tablesample (10 rows);

select key,dummy1,dummy2 from dummy_n6;

explain
select max('pants'), max('pANTS') from src group by key limit 1;
select max('pants'), max('pANTS') from src group by key limit 1;
