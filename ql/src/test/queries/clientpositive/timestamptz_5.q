set hive.cbo.enable=false;

drop table if exists timestamplocaltz_n1;
drop table if exists timestamplocaltz_n2;

create table timestamplocaltz_n1 (key int, dd timestamp with local time zone);
create table timestamplocaltz_n2 (key int, dd timestamp with local time zone);

-- between clause with timestamp literal in join condition
select d1.key, d2.dd
  from (select key, dd as start_dd, current_timestamp as end_dd from timestamplocaltz_n1) d1
  join timestamplocaltz_n2 as d2
    on d1.key = d2.key or d2.dd between timestamplocaltz '2010-04-01 00:00:00 America/Los_Angeles' and timestamplocaltz '2010-04-02 00:00:00 America/Los_Angeles';

drop table timestamplocaltz_n1;
drop table timestamplocaltz_n2;