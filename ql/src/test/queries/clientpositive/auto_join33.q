set hive.auto.convert.join=true;
set hive.optimize.mapjoin.mapreduce=true;

-- empty tables
create table studenttab10k (name string, age int, gpa double);
create table votertab10k (name string, age int, registration string, contributions float);

explain select s.name, count(distinct registration)
from studenttab10k s join votertab10k v
on (s.name = v.name)
group by s.name;

select s.name, count(distinct registration)
from studenttab10k s join votertab10k v
on (s.name = v.name)
group by s.name;
