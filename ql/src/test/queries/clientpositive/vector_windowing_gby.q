--! qt:dataset:cbo_t3
--! qt:dataset:alltypesorc
set hive.explain.user=false;
set hive.cli.print.header=true;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
set hive.vectorized.execution.ptf.enabled=true;
set hive.fetch.task.conversion=none;

set hive.mapred.mode=nonstrict;

explain vectorization detail
       select rank() over (order by return_ratio) as return_rank from
       (select sum(wr.cint)/sum(ws.c_int)  as return_ratio
                 from cbo_t3  ws join alltypesorc wr on ws.value = wr.cstring1
                  group by ws.c_boolean ) in_web
;

       select rank() over (order by return_ratio) as return_rank from
       (select sum(wr.cint)/sum(ws.c_int)  as return_ratio
                 from cbo_t3  ws join alltypesorc wr on ws.value = wr.cstring1
                  group by ws.c_boolean ) in_web
;
