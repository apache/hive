set hive.map.aggr=true;

-- SORT_QUERY_RESULTS

create table dest1(key int, cnt int);
create table dest2(key int, cnt int);

explain
from src
insert overwrite table dest1 select key, count(distinct value) group by key
insert overwrite table dest2 select key+key, count(distinct value) group by key+key;

from src
insert overwrite table dest1 select key, count(distinct value) group by key
insert overwrite table dest2 select key+key, count(distinct value) group by key+key;


select * from dest1 where key < 10;
select * from dest2 where key < 20 order by key limit 10;

