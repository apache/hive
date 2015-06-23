set hive.explain.user=false;
-- SORT_QUERY_RESULTS

explain select distinct src.* from src;

select distinct src.* from src;

select distinct * from src;

explain select distinct * from src where key < '3';

select distinct * from src where key < '3';

from src a select distinct a.* where a.key = '238';

explain
SELECT distinct * from (
select * from src1
union all
select * from src )subq; 

SELECT distinct * from (
select * from src1
union all
select * from src )subq; 

drop view if exists sdi;

explain create view sdi as select distinct * from src order by key limit 2;

create view sdi as select distinct * from src order by key limit 2;

describe extended sdi;

describe formatted sdi;

select * from sdi;

select distinct * from src union all select distinct * from src1;

select distinct * from src join src1 on src.key=src1.key;

SELECT distinct *
FROM src1 x JOIN src y ON (x.key = y.key) 
JOIN srcpart z ON (x.value = z.value and z.ds='2008-04-08' and z.hr=11);

select * from (select distinct * from src)src1 
join 
(select distinct * from src)src2 
on src1.key=src2.key;

select distinct * from (select distinct * from src)src1;

explain select distinct src.* from src;

select distinct src.* from src;

select distinct * from src;

explain select distinct * from src where key < '3';

select distinct * from src where key < '3';

from src a select distinct a.* where a.key = '238';

explain
SELECT distinct * from (
select * from src1
union all
select * from src )subq; 

SELECT distinct * from (
select * from src1
union all
select * from src )subq; 

drop view if exists sdi;

explain create view sdi as select distinct * from src order by key limit 2;

create view sdi as select distinct * from src order by key limit 2;

describe extended sdi;

describe formatted sdi;

select * from sdi;

select distinct * from src union all select distinct * from src1;

select distinct * from src join src1 on src.key=src1.key;

SELECT distinct *
FROM src1 x JOIN src y ON (x.key = y.key) 
JOIN srcpart z ON (x.value = z.value and z.ds='2008-04-08' and z.hr=11);

select * from (select distinct * from src)src1 
join 
(select distinct * from src)src2 
on src1.key=src2.key;

select distinct * from (select distinct * from src)src1;
