drop table if exists date_1;
drop table if exists date_2;

create table date_1 (key int, dd date);
create table date_2 (key int, dd date);

-- between clause with date literal in join condition
select d1.key, d2.dd
  from (select key, dd as start_dd, current_date as end_dd from date_1) d1
  join date_2 as d2
    on d1.key = d2.key
    where d2.dd between start_dd and end_dd;

drop table date_1;
drop table date_2;