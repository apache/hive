SET hive.exec.dynamic.partition.mode=nonstrict;
set metastore.integral.jdo.pushdown=true;

create temporary table ptestfilter_n2_temp (a string, b int) partitioned by (c string);
INSERT OVERWRITE TABLE ptestfilter_n2_temp PARTITION (c) select 'Col1', 1, null;
alter table ptestfilter_n2_temp add partition (c=3);
alter table ptestfilter_n2_temp add partition (c=5);
show partitions ptestfilter_n2_temp;

--alter table ptestfilter_n2_temp drop partition(c = '__HIVE_DEFAULT_PARTITION__');
alter table ptestfilter_n2_temp drop partition(c = 3);
show partitions ptestfilter_n2_temp;

INSERT OVERWRITE TABLE ptestfilter_n2_temp PARTITION (c) select 'Col1', 1, null;
alter table ptestfilter_n2_temp drop partition(c != '__HIVE_DEFAULT_PARTITION__');
show partitions ptestfilter_n2_temp;

drop table ptestfilter_n2_temp;

create temporary table ptestfilter_n2_temp (a string, b int) partitioned by (c string, d int);
INSERT OVERWRITE TABLE ptestfilter_n2_temp PARTITION (c,d) select 'Col1', 1, null, null;
INSERT OVERWRITE TABLE ptestfilter_n2_temp PARTITION (c,d) select 'Col2', 2, null, 2;
INSERT OVERWRITE TABLE ptestfilter_n2_temp PARTITION (c,d) select 'Col3', 3, 'Uganda', null;
alter table ptestfilter_n2_temp add partition (c='Germany', d=2);
show partitions ptestfilter_n2_temp;

alter table ptestfilter_n2_temp drop partition (c='__HIVE_DEFAULT_PARTITION__');
alter table ptestfilter_n2_temp drop partition (c='Uganda', d='__HIVE_DEFAULT_PARTITION__');
alter table ptestfilter_n2_temp drop partition (c='Germany', d=2);
show partitions ptestfilter_n2_temp;

INSERT OVERWRITE TABLE ptestfilter_n2_temp PARTITION (c,d) select 'Col2', 2, null, 2;
INSERT OVERWRITE TABLE ptestfilter_n2_temp PARTITION (c,d) select 'Col2', 2, null, 3;
INSERT OVERWRITE TABLE ptestfilter_n2_temp PARTITION (c,d) select 'Col3', 3, 'Uganda', null;
alter table ptestfilter_n2_temp drop partition (d != 3);
show partitions ptestfilter_n2_temp;

drop table ptestfilter_n2_temp;


