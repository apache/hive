--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.exec.reducers.bytes.per.reducer=256;
set hive.optimize.sampling.orderby=true;

-- SORT_QUERY_RESULTS

create table e1_n3 (key string, value string);
create table e2_n4 (key string);

--test orderby+limit case
explain
select key,value from src order by key limit 10;
select key,value from src order by key limit 10;


--test orderby+limit+multi_insert case
explain FROM (select key,value from src order by key limit 10) a
INSERT OVERWRITE TABLE e1_n3
    SELECT key, value
INSERT OVERWRITE TABLE e2_n4
    SELECT key;

FROM (select key,value from src order by key limit 10) a
INSERT OVERWRITE TABLE e1_n3
    SELECT key, value
INSERT OVERWRITE TABLE e2_n4
    SELECT key;

select * from e1_n3;
select * from e2_n4;

--test orderby in multi_insert case
explain FROM (select key,value from src order by key) a
INSERT OVERWRITE TABLE e1_n3
    select key,value
INSERT OVERWRITE TABLE e2_n4
    select key;

FROM (select key,value from src order by key) a
INSERT OVERWRITE TABLE e1_n3 
    select key,value
INSERT OVERWRITE TABLE e2_n4 
    select key;

select * from e1_n3;
select * from e2_n4;

--test limit in subquery of multi_insert case
explain FROM (select key,value from src order by key) a
INSERT OVERWRITE TABLE e1_n3
    select key,value limit 10
INSERT OVERWRITE TABLE e2_n4
    select key;

FROM (select key,value from src order by key) a
INSERT OVERWRITE TABLE e1_n3 
    select key,value limit 10
INSERT OVERWRITE TABLE e2_n4 
    select key;

-- the result of e1_n3 is not the top 10, just randomly get 10 elements,so count the number of e1_n3
--select * from e1_n3;
select count(*) from e1_n3;
select * from e2_n4;

drop table e1_n3;
drop table e2_n4;
