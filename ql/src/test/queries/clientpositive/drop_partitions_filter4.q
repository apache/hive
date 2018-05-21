SET hive.exec.dynamic.partition.mode=nonstrict;

create table ptestfilter_n2 (a string, b int) partitioned by (c double);
INSERT OVERWRITE TABLE ptestfilter_n2 PARTITION (c) select 'Col1', 1, null;
alter table ptestfilter_n2 add partition (c=3.4);
alter table ptestfilter_n2 add partition (c=5.55);
show partitions ptestfilter_n2;

alter table ptestfilter_n2 drop partition(c = '__HIVE_DEFAULT_PARTITION__');
alter table ptestfilter_n2 drop partition(c = 3.40);
show partitions ptestfilter_n2;

INSERT OVERWRITE TABLE ptestfilter_n2 PARTITION (c) select 'Col1', 1, null;
alter table ptestfilter_n2 drop partition(c != '__HIVE_DEFAULT_PARTITION__');
show partitions ptestfilter_n2;

drop table ptestfilter_n2;

create table ptestfilter_n2 (a string, b int) partitioned by (c string, d int);
INSERT OVERWRITE TABLE ptestfilter_n2 PARTITION (c,d) select 'Col1', 1, null, null;
INSERT OVERWRITE TABLE ptestfilter_n2 PARTITION (c,d) select 'Col2', 2, null, 2;
INSERT OVERWRITE TABLE ptestfilter_n2 PARTITION (c,d) select 'Col3', 3, 'Uganda', null;
alter table ptestfilter_n2 add partition (c='Germany', d=2);
show partitions ptestfilter_n2;

alter table ptestfilter_n2 drop partition (c='__HIVE_DEFAULT_PARTITION__');
alter table ptestfilter_n2 drop partition (c='Uganda', d='__HIVE_DEFAULT_PARTITION__');
alter table ptestfilter_n2 drop partition (c='Germany', d=2);
show partitions ptestfilter_n2;

INSERT OVERWRITE TABLE ptestfilter_n2 PARTITION (c,d) select 'Col2', 2, null, 2;
INSERT OVERWRITE TABLE ptestfilter_n2 PARTITION (c,d) select 'Col2', 2, null, 3;
INSERT OVERWRITE TABLE ptestfilter_n2 PARTITION (c,d) select 'Col3', 3, 'Uganda', null;
alter table ptestfilter_n2 drop partition (d != 3);
show partitions ptestfilter_n2;

drop table ptestfilter_n2;


