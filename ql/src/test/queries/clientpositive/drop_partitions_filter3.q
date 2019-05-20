create table ptestfilter_n3 (a string, b int) partitioned by (c string, d int);
describe ptestfilter_n3;

alter table ptestfilter_n3 add partition (c='1', d=1);
alter table ptestfilter_n3 add partition (c='1', d=2);
alter table ptestFilter_n3 add partition (c='2', d=1);
alter table ptestfilter_n3 add partition (c='2', d=2);
alter table ptestfilter_n3 add partition (c='3', d=1);
alter table ptestfilter_n3 add partition (c='3', d=2);
show partitions ptestfilter_n3;

alter table ptestfilter_n3 drop partition (c='1', d=1);
show partitions ptestfilter_n3;

alter table ptestfilter_n3 drop partition (c='2');
show partitions ptestfilter_n3;

drop table ptestfilter_n3;


