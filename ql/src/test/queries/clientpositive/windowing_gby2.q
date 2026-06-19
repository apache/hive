--! qt:dataset:cbo_t3
--! qt:dataset:alltypesorc
set hive.mapred.mode=nonstrict;

explain
select rank() over (order by sum(ws.c_int)) as return_rank
from cbo_t3 ws
group by ws.key;

select rank() over (order by sum(ws.c_int)) as return_rank
from cbo_t3 ws
group by ws.key;

explain
select avg(cast(ws.key as int)) over (partition by min(ws.value) order by sum(ws.c_int)) as return_rank
from cbo_t3 ws
group by cast(ws.key as int);

select avg(cast(ws.key as int)) over (partition by min(ws.value) order by sum(ws.c_int)) as return_rank
from cbo_t3 ws
group by cast(ws.key as int);

explain
select rank () over(partition by key order by sum(c_int - c_float) desc) ,
dense_rank () over(partition by lower(value) order by sum(c_float/c_int) asc),
percent_rank () over(partition by max(c_int) order by sum((c_float/c_int) - c_int) asc)
from cbo_t3
group by key, value;

select rank () over(partition by key order by sum(c_int - c_float) desc) ,
dense_rank () over(partition by lower(value) order by sum(c_float/c_int) asc),
percent_rank () over(partition by max(c_int) order by sum((c_float/c_int) - c_int) asc)
from cbo_t3
group by key, value;

explain
select rank() over (order by sum(wr.cint)/sum(ws.c_int)) as return_rank
from cbo_t3 ws join alltypesorc wr on ws.value = wr.cstring1
group by ws.c_boolean;

select rank() over (order by sum(wr.cint)/sum(ws.c_int)) as return_rank
from cbo_t3 ws join alltypesorc wr on ws.value = wr.cstring1
group by ws.c_boolean;
