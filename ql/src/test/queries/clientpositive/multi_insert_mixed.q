create table src_multi1 like src;
create table src_multi2 like src;
create table src_multi3 like src;
set hive.stats.dbclass=fs;
-- Testing the case where a map work contains both shuffling (ReduceSinkOperator)
-- and inserting to output table (FileSinkOperator).

explain
from src
insert overwrite table src_multi1 select key, count(1) group by key order by key
insert overwrite table src_multi2 select value, count(1) group by value order by value
insert overwrite table src_multi3 select * where key < 10;

from src
insert overwrite table src_multi1 select key, count(1) group by key order by key
insert overwrite table src_multi2 select value, count(1) group by value order by value
insert overwrite table src_multi3 select * where key < 10;

select * from src_multi1;
select * from src_multi2;
select * from src_multi3;
