set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
set hive.fetch.task.conversion=none;
set hive.cli.print.header=true;

CREATE TABLE T1_text_n2(key INT, value INT) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/groupby_groupingid.txt' INTO TABLE T1_text_n2;

CREATE TABLE T1_n47 STORED AS ORC AS SELECT * FROM T1_text_n2;

-- SORT_QUERY_RESULTS

explain vectorization detail
select key, value, `grouping__id`, grouping(key), grouping(value)
from T1_n47
group by rollup(key, value);

select key, value, `grouping__id`, grouping(key), grouping(value)
from T1_n47
group by rollup(key, value);

explain vectorization detail
select key, value, `grouping__id`, grouping(key), grouping(value)
from T1_n47
group by cube(key, value);

select key, value, `grouping__id`, grouping(key), grouping(value)
from T1_n47
group by cube(key, value);

explain vectorization detail
select key, value
from T1_n47
group by cube(key, value)
having grouping(key) = 1;

select key, value
from T1_n47
group by cube(key, value)
having grouping(key) = 1;

explain vectorization detail
select key, value, grouping(key)+grouping(value) as x
from T1_n47
group by cube(key, value)
having grouping(key) = 1 OR grouping(value) = 1
order by x desc, case when x = 1 then key end;

select key, value, grouping(key)+grouping(value) as x
from T1_n47
group by cube(key, value)
having grouping(key) = 1 OR grouping(value) = 1
order by x desc, case when x = 1 then key end;

set hive.cbo.enable=false;

explain vectorization detail
select key, value, `grouping__id`, grouping(key), grouping(value)
from T1_n47
group by rollup(key, value);

select key, value, `grouping__id`, grouping(key), grouping(value)
from T1_n47
group by rollup(key, value);

explain vectorization detail
select key, value, `grouping__id`, grouping(key), grouping(value)
from T1_n47
group by cube(key, value);

select key, value, `grouping__id`, grouping(key), grouping(value)
from T1_n47
group by cube(key, value);

explain vectorization detail
select key, value
from T1_n47
group by cube(key, value)
having grouping(key) = 1;

select key, value
from T1_n47
group by cube(key, value)
having grouping(key) = 1;

explain vectorization detail
select key, value, grouping(key)+grouping(value) as x
from T1_n47
group by cube(key, value)
having grouping(key) = 1 OR grouping(value) = 1
order by x desc, case when x = 1 then key end;

select key, value, grouping(key)+grouping(value) as x
from T1_n47
group by cube(key, value)
having grouping(key) = 1 OR grouping(value) = 1
order by x desc, case when x = 1 then key end;

explain vectorization detail
select key, value, grouping(key), grouping(value)
from T1_n47
group by key, value;

select key, value, grouping(key), grouping(value)
from T1_n47
group by key, value;

explain vectorization detail
select key, value, grouping(value)
from T1_n47
group by key, value;

select key, value, grouping(value)
from T1_n47
group by key, value;

explain vectorization detail
select key, value
from T1_n47
group by key, value
having grouping(key) = 0;

select key, value
from T1_n47
group by key, value
having grouping(key) = 0;

explain vectorization detail
select key, value, `grouping__id`, grouping(key, value)
from T1_n47
group by cube(key, value);

select key, value, `grouping__id`, grouping(key, value)
from T1_n47
group by cube(key, value);

explain vectorization detail
select key, value, `grouping__id`, grouping(value, key)
from T1_n47
group by cube(key, value);

select key, value, `grouping__id`, grouping(value, key)
from T1_n47
group by cube(key, value);

explain vectorization detail
select key, value, `grouping__id`, grouping(key, value)
from T1_n47
group by rollup(key, value);

select key, value, `grouping__id`, grouping(key, value)
from T1_n47
group by rollup(key, value);

explain vectorization detail
select key, value, `grouping__id`, grouping(value, key)
from T1_n47
group by rollup(key, value);

select key, value, `grouping__id`, grouping(value, key)
from T1_n47
group by rollup(key, value);
