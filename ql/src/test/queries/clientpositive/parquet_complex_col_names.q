set hive.support.quoted.identifiers=column;
set hive.fetch.task.conversion=none;
set hive.mapred.mode=nonstrict;

set hive.vectorized.execution.enabled=true;

drop table if exists tbl_bps_src;
drop table if exists parquet_bps_sink;

create table tbl_bps_src (`rate(bps)` double, id int) stored as orc;
insert into tbl_bps_src values (12.5, 1), (25.0, 2), (37.5, 3);

create table parquet_bps_sink (`rate(bps)` double, id int) stored as parquet;

insert overwrite table parquet_bps_sink
select `rate(bps)`, id
from tbl_bps_src
order by id
limit 10;

select `rate(bps)`, id from parquet_bps_sink order by id;


set hive.vectorized.execution.enabled=false;

insert overwrite table parquet_bps_sink
select `rate(bps)`, id
from tbl_bps_src
order by id
limit 10;

select `rate(bps)`, id from parquet_bps_sink order by id;
