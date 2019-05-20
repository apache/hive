create table ptestfilter1 (a string, b int) partitioned by (c string);

alter table ptestfilter1 add partition (c='US');
show partitions ptestfilter1;

alter table ptestfilter1 drop partition (c > '__HIVE_DEFAULT_PARTITION__');

