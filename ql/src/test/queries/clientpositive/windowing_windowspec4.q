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

-- 0 preceding/following should be the same as current row
select type, i,
max(i) over (partition by type order by i rows between 1 preceding and 0 following),
min(i) over (partition by type order by i rows between 1 preceding and 0 following),
max(i) over (partition by type order by i rows between 0 preceding and 1 following),
min(i) over (partition by type order by i rows between 0 preceding and 1 following),
max(i) over (partition by type order by i rows between 0 preceding and 0 following),
min(i) over (partition by type order by i rows between 0 preceding and 0 following)
from smalltable_windowing;

select type, i,
max(i) over (partition by type order by i rows between 1 preceding and current row),
min(i) over (partition by type order by i rows between 1 preceding and current row),
max(i) over (partition by type order by i rows between current row and 1 following),
min(i) over (partition by type order by i rows between current row and 1 following),
max(i) over (partition by type order by i rows between current row and current row),
min(i) over (partition by type order by i rows between current row and current row)
from smalltable_windowing;
