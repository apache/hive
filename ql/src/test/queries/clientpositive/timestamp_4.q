drop table if exists timestamp_1;
drop table if exists timestamp_2;

create table timestamp_1 (key int, dd timestamp);
create table timestamp_2 (key int, dd timestamp);

-- between clause with timestamp literal in join condition
select d1.key, d2.dd
  from (select key, dd as start_dd, current_timestamp as end_dd from timestamp_1) d1
  join timestamp_2 as d2
    on d1.key = d2.key
    where d2.dd between start_dd and end_dd;

drop table timestamp_1;
drop table timestamp_2;