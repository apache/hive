SET hive.vectorized.execution.enabled=false;

-- SORT_QUERY_RESULTS

CREATE TABLE T1_n64(key INT, value INT) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/groupby_groupingid.txt' INTO TABLE T1_n64;

explain
select key, value, `grouping__id`, grouping(key), grouping(value)
from T1_n64
group by rollup(key, value);

select key, value, `grouping__id`, grouping(key), grouping(value)
from T1_n64
group by rollup(key, value);

explain
select key, value, `grouping__id`, grouping(key), grouping(value)
from T1_n64
group by cube(key, value);

select key, value, `grouping__id`, grouping(key), grouping(value)
from T1_n64
group by cube(key, value);

explain
select key, value
from T1_n64
group by cube(key, value)
having grouping(key) = 1;

select key, value
from T1_n64
group by cube(key, value)
having grouping(key) = 1;

explain
select key, value, grouping(key)+grouping(value) as x
from T1_n64
group by cube(key, value)
having grouping(key) = 1 OR grouping(value) = 1
order by x desc, case when x = 1 then key end;

select key, value, grouping(key)+grouping(value) as x
from T1_n64
group by cube(key, value)
having grouping(key) = 1 OR grouping(value) = 1
order by x desc, case when x = 1 then key end;

set hive.cbo.enable=false;

explain
select key, value, `grouping__id`, grouping(key), grouping(value)
from T1_n64
group by rollup(key, value);

select key, value, `grouping__id`, grouping(key), grouping(value)
from T1_n64
group by rollup(key, value);

explain
select key, value, `grouping__id`, grouping(key), grouping(value)
from T1_n64
group by cube(key, value);

select key, value, `grouping__id`, grouping(key), grouping(value)
from T1_n64
group by cube(key, value);

explain
select key, value
from T1_n64
group by cube(key, value)
having grouping(key) = 1;

select key, value
from T1_n64
group by cube(key, value)
having grouping(key) = 1;

explain
select key, value, grouping(key)+grouping(value) as x
from T1_n64
group by cube(key, value)
having grouping(key) = 1 OR grouping(value) = 1
order by x desc, case when x = 1 then key end;

select key, value, grouping(key)+grouping(value) as x
from T1_n64
group by cube(key, value)
having grouping(key) = 1 OR grouping(value) = 1
order by x desc, case when x = 1 then key end;

explain
select key, value, grouping(key), grouping(value)
from T1_n64
group by key, value;

select key, value, grouping(key), grouping(value)
from T1_n64
group by key, value;

explain
select key, value, grouping(value)
from T1_n64
group by key, value;

select key, value, grouping(value)
from T1_n64
group by key, value;

explain
select key, value
from T1_n64
group by key, value
having grouping(key) = 0;

select key, value
from T1_n64
group by key, value
having grouping(key) = 0;

explain
select key, value, `grouping__id`, grouping(key, value)
from T1_n64
group by cube(key, value);

select key, value, `grouping__id`, grouping(key, value)
from T1_n64
group by cube(key, value);

explain
select key, value, `grouping__id`, grouping(value, key)
from T1_n64
group by cube(key, value);

select key, value, `grouping__id`, grouping(value, key)
from T1_n64
group by cube(key, value);

explain
select key, value, `grouping__id`, grouping(key, value)
from T1_n64
group by rollup(key, value);

select key, value, `grouping__id`, grouping(key, value)
from T1_n64
group by rollup(key, value);

explain
select key, value, `grouping__id`, grouping(value, key)
from T1_n64
group by rollup(key, value);

select key, value, `grouping__id`, grouping(value, key)
from T1_n64
group by rollup(key, value);
