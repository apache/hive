SET hive.metastore.batch.retrieve.max=1;
create table if not exists temp_n0(col STRING) partitioned by (p STRING);
alter table temp_n0 add if not exists partition (p ='p1');
alter table temp_n0 add if not exists partition (p ='p2');
alter table temp_n0 add if not exists partition (p ='p3');

show partitions temp_n0;

drop table temp_n0;

create table if not exists temp_n0(col STRING) partitioned by (p STRING);

show partitions temp_n0;

drop table temp_n0;
