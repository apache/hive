--! qt:dataset:cbo_t3
--! qt:dataset:alltypesorc
set hive.mapred.mode=nonstrict;
set hive.explain.user=true;
explain
       select rank() over (order by return_ratio) as return_rank from
       (select sum(wr.cint)/sum(ws.c_int)  as return_ratio
                 from cbo_t3  ws join alltypesorc wr on ws.value = wr.cstring1
                  group by ws.c_boolean ) in_web
;
