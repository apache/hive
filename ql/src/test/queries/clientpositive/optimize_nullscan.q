--! qt:dataset:srcpart
--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.cbo.enable=false;
-- SORT_QUERY_RESULTS

-- Disable CBO here, because it messes with the cases specifically crafted for the optimizer.
-- Instead, we could improve the optimizer to recognize more cases, e.g. filter before join.

explain extended 
select key from src where false;
select key from src where false;

explain extended
select count(key) from srcpart where 1=2 group by key;
select count(key) from srcpart where 1=2 group by key;

explain extended
select * from (select key from src where false) a left outer join (select key from srcpart limit 0) b on a.key=b.key;
select * from (select key from src where false) a left outer join (select key from srcpart limit 0) b on a.key=b.key;

explain extended
select count(key) from src where false union all select count(key) from srcpart ;
select count(key) from src where false union all select count(key) from srcpart ;

explain extended
select * from (select key from src where false) a left outer join (select value from srcpart limit 0) b ;
select * from (select key from src where false) a left outer join (select value from srcpart limit 0) b ;

explain extended 
select * from (select key from src union all select src.key from src left outer join srcpart on src.key = srcpart.key) a  where false;
select * from (select key from src union all select src.key from src left outer join srcpart on src.key = srcpart.key) a  where false;

explain extended 
select * from src s1, src s2 where false and s1.value = s2.value;
select * from src s1, src s2 where false and s1.value = s2.value;

explain extended
select count(1) from src where null = 1;
select count(1) from src where null = 1;
