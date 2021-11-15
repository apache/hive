set hive.fileformat.check=true;

create temporary table supply_temp (id int, part string, quantity int) partitioned by (day int)
stored as orc
location 'hdfs:///tmp/a1'
;

explain alter table supply_temp add partition (day=20110102) location
'hdfs:///tmp/a2';

alter table supply_temp add partition (day=20110103) location
'hdfs:///tmp/a3';

show partitions supply_temp;

drop table supply_temp;