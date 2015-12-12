--Test small dataset with larger windowing

drop table if exists smalltable_windowing;

create table smalltable_windowing(
      i int,
      type string);
insert into smalltable_windowing values(3, 'a'), (1, 'a'), (2, 'a');

select type, i,
max(i) over (partition by type order by i rows between 1 preceding and 7 following),
min(i) over (partition by type order by i rows between 1 preceding and 7 following),
first_value(i) over (partition by type order by i rows between 1 preceding and 7 following),
last_value(i) over (partition by type order by i rows between 1 preceding and 7 following),
avg(i) over (partition by type order by i rows between 1 preceding and 7 following),
sum(i) over (partition by type order by i rows between 1 preceding and 7 following),
collect_set(i) over (partition by type order by i rows between 1 preceding and 7 following),
count(i) over (partition by type order by i rows between 1 preceding and 7 following)
from smalltable_windowing;
