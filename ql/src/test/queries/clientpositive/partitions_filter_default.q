SET hive.exec.dynamic.partition.mode=nonstrict;

create table ptestfilter (a string) partitioned by (c int);
INSERT OVERWRITE TABLE ptestfilter PARTITION (c) select 'Col1', null;
INSERT OVERWRITE TABLE ptestfilter PARTITION (c) select 'Col2', 5;
show partitions ptestfilter;

select * from ptestfilter;

select * from ptestfilter where c between 2 and 6 ;

drop table ptestfilter;


