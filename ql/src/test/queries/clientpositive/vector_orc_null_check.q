--! qt:dataset:src
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

create table listtable_n0(l array<string>);
create table listtable_orc_n0(l array<string>) stored as orc;

insert overwrite table listtable_n0 select array(null) from src;
insert overwrite table listtable_orc_n0 select * from listtable_n0;

explain vectorization expression
select size(l) from listtable_orc_n0 limit 10;
select size(l) from listtable_orc_n0 limit 10;

