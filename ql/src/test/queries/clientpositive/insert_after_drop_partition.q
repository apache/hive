--! qt:dataset:src
create external table insert_after_drop_partition(key string, val string) partitioned by (insertdate string);

insert overwrite table insert_after_drop_partition partition (insertdate='2008-01-01') select * from src limit 10;

alter table insert_after_drop_partition drop partition (insertdate='2008-01-01');

insert overwrite table insert_after_drop_partition partition (insertdate='2008-01-01') select * from src limit 10;