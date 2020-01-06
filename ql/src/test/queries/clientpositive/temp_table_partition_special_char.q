--! qt:dataset:src
create temporary table sc_temp as select *
                   from (select '2011-01-11', '2011-01-11+14:18:26' from src tablesample (1 rows)
                   union all
                   select '2011-01-11', '2011-01-11+15:18:26' from src tablesample (1 rows)
                   union all
                   select '2011-01-11', '2011-01-11+16:18:26' from src tablesample (1 rows) ) s;

create temporary table sc_part_temp (key string) partitioned by (ts string) stored as rcfile;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table sc_part_temp partition(ts) select * from sc_temp;
show partitions sc_part_temp;
select count(*) from sc_part_temp where ts is not null;

insert overwrite table sc_part_temp partition(ts) select * from sc_temp;
show partitions sc_part_temp;
select count(*) from sc_part_temp where ts is not null;
