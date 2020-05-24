--! qt:dataset:src
create temporary table sc_n0_temp as select *
                      from (select '2011-01-11', '2011-01-11+14:18:26' from src tablesample (1 rows)
                      union all
                      select '2011-01-11', '2011-01-11+15:18:26' from src tablesample (1 rows)
                      union all
                      select '2011-01-11', '2011-01-11+16:18:26' from src tablesample (1 rows) ) s;

create table sc_part_n0_temp (key string) partitioned by (ts string) stored as rcfile;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

set hive.decode.partition.name=false;
insert overwrite table sc_part_n0_temp partition(ts) select * from sc_n0_temp;
show partitions sc_part_n0_temp;
select count(*) from sc_part_n0_temp where ts is not null;

set hive.decode.partition.name=true;
insert overwrite table sc_part_n0_temp partition(ts) select * from sc_n0_temp;
show partitions sc_part_n0_temp;
select count(*) from sc_part_n0_temp where ts is not null;
