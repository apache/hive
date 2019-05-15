create table count_distinct_test(id int,key int,name int);

insert into count_distinct_test values (1,1,2),(1,2,3),(1,3,2),(1,4,2),(1,5,3);

-- simple case; no need for opt
explain select id,count(distinct key),count(distinct name)
from count_distinct_test
group by id;

select id,count(distinct key),count(distinct name)
from count_distinct_test
group by id;

-- dedup on
set hive.optimize.reducededuplication=true;

-- candidate1
explain select id,count(Distinct key),count(Distinct name)
from (select id,key,name from count_distinct_test group by id,key,name)m
group by id;

select id,count(Distinct key),count(Distinct name)
from (select id,key,name from count_distinct_test group by id,key,name)m
group by id;

-- candidate2
explain select id,count(Distinct name),count(Distinct key)
from (select id,key,name from count_distinct_test group by id,name,key)m
group by id;

select id,count(Distinct name),count(Distinct key)
from (select id,key,name from count_distinct_test group by id,name,key)m
group by id;

-- deduplication off
set hive.optimize.reducededuplication=false;

-- candidate1
explain select id,count(Distinct key),count(Distinct name)
from (select id,key,name from count_distinct_test group by id,key,name)m
group by id;

select id,count(Distinct key),count(Distinct name)
from (select id,key,name from count_distinct_test group by id,key,name)m
group by id;

-- candidate2
explain select id,count(Distinct name),count(Distinct key)
from (select id,key,name from count_distinct_test group by id,name,key)m
group by id;

select id,count(Distinct name),count(Distinct key)
from (select id,key,name from count_distinct_test group by id,name,key)m
group by id;
