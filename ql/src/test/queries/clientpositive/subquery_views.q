--! qt:dataset:src
set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS

-- exists test
create view cv1 as 
select * 
from src b 
where exists
  (select a.key 
  from src a 
  where b.value = a.value  and a.key = b.key and a.value > 'val_9')
;

describe extended cv1;

select * 
from cv1 where cv1.key in (select key from cv1 c where c.key > '95');
;


-- not in test
create view cv2 as 
select * 
from src b 
where b.key not in
  (select a.key 
  from src a 
  where b.value = a.value  and a.key = b.key and a.value > 'val_11'
  )
;

describe extended cv2;

explain
select * 
from cv2 where cv2.key in (select key from cv2 c where c.key < '11');
;

select * 
from cv2 where cv2.key in (select key from cv2 c where c.key < '11');
;

-- in where + having
create view cv3 as
select key, value, count(*) 
from src b
where b.key in (select key from src where src.key > '8')
group by key, value
having count(*) in (select count(*) from src s1 where s1.key > '9' group by s1.key )
;

describe extended cv3;

select * from cv3;


-- join of subquery views
select *
from cv3
where cv3.key in (select key from cv1);

drop table tc;

create table tc (`@d` int);

insert overwrite table tc select 1 from src limit 1;

drop view tcv;

create view tcv as select * from tc b where exists (select a.`@d` from tc a where b.`@d`=a.`@d`);

describe extended tcv;

select * from tcv;