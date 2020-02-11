--! qt:dataset:src
set hive.cbo.returnpath.hiveop=true;
-- SORT_QUERY_RESULTS

CREATE TABLE u1_n0 as select key, value from src order by key limit 5;

CREATE TABLE u2_n0 as select key, value from src order by key limit 3;

CREATE TABLE u3_n0 as select key, value from src order by key desc limit 5;

select * from u1_n0;

select * from u2_n0;

select * from u3_n0;

select key, value from 
(
select key, value from u1_n0
union all
select key, value from u2_n0
union all
select key as key, value from u3_n0
) tab;

select key, value from 
(
select key, value from u1_n0
union 
select key, value from u2_n0
union all
select key, value from u3_n0
) tab;

select key, value from 
(
select key, value from u1_n0
union distinct
select key, value from u2_n0
union all
select key as key, value from u3_n0
) tab;

select key, value from 
(
select key, value from u1_n0
union all
select key, value from u2_n0
union
select key, value from u3_n0
) tab;

select key, value from 
(
select key, value from u1_n0
union 
select key, value from u2_n0
union
select key as key, value from u3_n0
) tab;

select distinct * from 
(
select key, value from u1_n0
union all 
select key, value from u2_n0
union all
select key as key, value from u3_n0
) tab;

select distinct * from 
(
select distinct * from u1_n0
union  
select key, value from u2_n0
union all
select key as key, value from u3_n0
) tab;

drop view if exists v_n14;

set hive.cbo.returnpath.hiveop=false;
create view v_n14 as select distinct * from 
(
select distinct * from u1_n0
union  
select key, value from u2_n0
union all
select key as key, value from u3_n0
) tab;

describe extended v_n14;

select * from v_n14;

drop view if exists v_n14;

create view v_n14 as select tab.* from 
(
select distinct * from u1_n0
union  
select distinct * from u2_n0
) tab;

describe extended v_n14;

select * from v_n14;

drop view if exists v_n14;

create view v_n14 as select * from 
(
select distinct u1_n0.* from u1_n0
union all 
select distinct * from u2_n0
) tab;

describe extended v_n14;

select * from v_n14;

select distinct * from 
(
select key, value from u1_n0
union all 
select key, value from u2_n0
union 
select key as key, value from u3_n0
) tab;

