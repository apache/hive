--! qt:dataset:src
-- SORT_QUERY_RESULTS

CREATE TABLE u1 as select key, value from src order by key limit 5;

CREATE TABLE u2 as select key, value from src order by key limit 3;

CREATE TABLE u3 as select key, value from src order by key desc limit 5;

select key, value from 
(
select key, value from u1
union 
select key, value from u2
union all
select key, value from u3
) tab;

select key, value from 
(
select key, value from u1
union distinct
select key, value from u2
union all
select key as key, value from u3
) tab;

select key, value from 
(
select key, value from u1
union all
select key, value from u2
union
select key, value from u3
) tab;

select key, value from 
(
select key, value from u1
union 
select key, value from u2
union
select key as key, value from u3
) tab;


select distinct * from 
(
select distinct * from u1
union  
select key, value from u2
union all
select key as key, value from u3
) tab;

drop view if exists v_n12;

create view v_n12 as select distinct * from 
(
select distinct * from u1
union  
select key, value from u2
union all
select key as key, value from u3
) tab;

describe extended v_n12;

select * from v_n12;

drop view if exists v_n12;

create view v_n12 as select tab.* from 
(
select distinct * from u1
union  
select distinct * from u2
) tab;

describe extended v_n12;

select * from v_n12;

drop view if exists v_n12;

create view v_n12 as select * from 
(
select distinct u1.* from u1
union all 
select distinct * from u2
) tab;

describe extended v_n12;

select * from v_n12;

select distinct * from 
(
select key, value from u1
union all 
select key, value from u2
union 
select key as key, value from u3
) tab;

