set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=1431655765;


create table testtable_n1000 (s_state string, ss_net_profit double);
insert into testtable_n1000 values
  ('AA', 101),
  ('AB', 102),
  ('AC', 103),
  ('AD', 104),
  ('AE', 105),
  ('AF', 106),
  ('AG', 107),
  ('AH', 108),
  ('AI', 109),
  ('AJ', 110);

explain
select s_state, ranking
from (
  select s_state as s_state,
    sum(ss_net_profit),
    rank() over ( partition by s_state order by sum(ss_net_profit) desc) as ranking
  from testtable_n1000
  group by s_state) tmp1
where ranking <= 5;

select s_state, ranking
from (
  select s_state as s_state,
    sum(ss_net_profit),
    rank() over ( partition by s_state order by sum(ss_net_profit) desc) as ranking
  from testtable_n1000
  group by s_state) tmp1
where ranking <= 5;

drop table testtable_n1000;
