--Test small dataset with larger windowing

set hive.cli.print.header=true;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
set hive.vectorized.execution.ptf.enabled=true;
set hive.fetch.task.conversion=none;

drop table if exists smalltable_windowing_n0;

create table smalltable_windowing_n0(
      i int,
      type string);
insert into smalltable_windowing_n0 values(3, 'a'), (1, 'a'), (2, 'a');

explain vectorization detail
select type, i,
max(i) over (partition by type order by i rows between 1 preceding and 7 following),
min(i) over (partition by type order by i rows between 1 preceding and 7 following),
first_value(i) over (partition by type order by i rows between 1 preceding and 7 following),
last_value(i) over (partition by type order by i rows between 1 preceding and 7 following),
avg(i) over (partition by type order by i rows between 1 preceding and 7 following),
sum(i) over (partition by type order by i rows between 1 preceding and 7 following),
collect_set(i) over (partition by type order by i rows between 1 preceding and 7 following),
count(i) over (partition by type order by i rows between 1 preceding and 7 following)
from smalltable_windowing_n0;

select type, i,
max(i) over (partition by type order by i rows between 1 preceding and 7 following),
min(i) over (partition by type order by i rows between 1 preceding and 7 following),
first_value(i) over (partition by type order by i rows between 1 preceding and 7 following),
last_value(i) over (partition by type order by i rows between 1 preceding and 7 following),
avg(i) over (partition by type order by i rows between 1 preceding and 7 following),
sum(i) over (partition by type order by i rows between 1 preceding and 7 following),
collect_set(i) over (partition by type order by i rows between 1 preceding and 7 following),
count(i) over (partition by type order by i rows between 1 preceding and 7 following)
from smalltable_windowing_n0;
