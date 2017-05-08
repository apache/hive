set hive.cbo.returnpath.hiveop=true;
-- SORT_QUERY_RESULTS

CREATE TABLE u1 as select key, value from src order by key limit 5;

CREATE TABLE u2 as select key, value from src order by key limit 3;

CREATE TABLE u3 as select key, value from src order by key desc limit 5;

select * from u1;

select * from u2;

select * from u3;

select key, value from 
(
select key, value from u1
union all
select key, value from u2
union all
select key as key, value from u3
) tab;

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
select key, value from u1
union all 
select key, value from u2
union all
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

drop view if exists v;

create view v as select distinct * from 
(
select distinct * from u1
union  
select key, value from u2
union all
select key as key, value from u3
) tab;

describe extended v;

select * from v;

drop view if exists v;

create view v as select tab.* from 
(
select distinct * from u1
union  
select distinct * from u2
) tab;

describe extended v;

select * from v;

drop view if exists v;

create view v as select * from 
(
select distinct u1.* from u1
union all 
select distinct * from u2
) tab;

describe extended v;

select * from v;

select distinct * from 
(
select key, value from u1
union all 
select key, value from u2
union 
select key as key, value from u3
) tab;

