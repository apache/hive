SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

create table listtable(l array<string>);
create table listtable_orc(l array<string>) stored as orc;

insert overwrite table listtable select array(null) from src;
insert overwrite table listtable_orc select * from listtable;

explain vectorization expression
select size(l) from listtable_orc limit 10;
select size(l) from listtable_orc limit 10;

