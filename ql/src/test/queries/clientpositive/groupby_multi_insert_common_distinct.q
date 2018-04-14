--! qt:dataset:src
set hive.map.aggr=true;

-- SORT_QUERY_RESULTS

create table dest1_n99(key int, cnt int);
create table dest2_n27(key int, cnt int);

explain
from src
insert overwrite table dest1_n99 select key, count(distinct value) group by key
insert overwrite table dest2_n27 select key+key, count(distinct value) group by key+key;

from src
insert overwrite table dest1_n99 select key, count(distinct value) group by key
insert overwrite table dest2_n27 select key+key, count(distinct value) group by key+key;


select * from dest1_n99 where key < 10;
select * from dest2_n27 where key < 20 order by key limit 10;

