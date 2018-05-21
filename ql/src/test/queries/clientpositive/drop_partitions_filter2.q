create table ptestfilter_n0 (a string, b int) partitioned by (c int, d int);
describe ptestfilter_n0;

alter table ptestfilter_n0 add partition (c=1, d=1);
alter table ptestfilter_n0 add partition (c=1, d=2);
alter table ptestFilter_n0 add partition (c=2, d=1);
alter table ptestfilter_n0 add partition (c=2, d=2);
alter table ptestfilter_n0 add partition (c=3, d=1);
alter table ptestfilter_n0 add partition (c=30, d=2);
show partitions ptestfilter_n0;

alter table ptestfilter_n0 drop partition (c=1, d=1);
show partitions ptestfilter_n0;

alter table ptestfilter_n0 drop partition (c=2);
show partitions ptestfilter_n0;

alter table ptestfilter_n0 drop partition (c<4);
show partitions ptestfilter_n0;

drop table ptestfilter_n0;


