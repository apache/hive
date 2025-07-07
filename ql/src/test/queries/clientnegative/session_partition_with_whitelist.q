set metaconf:metastore.partition.name.whitelist.pattern;

create table t1 (id int) partitioned by (pcol string);
alter table t1 add partition (pCol='2025-06-09');

set metaconf:metastore.partition.name.whitelist.pattern=[^9]*;
alter table t1 add partition (pCol='2025-06-19');
show partitions t1;

