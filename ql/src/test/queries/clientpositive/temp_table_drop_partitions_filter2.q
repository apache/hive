set metastore.integral.jdo.pushdown=true;
create temporary table ptestfilter_n0_temp (a string, b int) partitioned by (c int, d int);
describe ptestfilter_n0_temp;

alter table ptestfilter_n0_temp add partition (c=1, d=1);
alter table ptestfilter_n0_temp add partition (c=1, d=2);
alter table ptestfilter_n0_temp add partition (c=2, d=1);
alter table ptestfilter_n0_temp add partition (c=2, d=2);
alter table ptestfilter_n0_temp add partition (c=3, d=1);
alter table ptestfilter_n0_temp add partition (c=30, d=2);
show partitions ptestfilter_n0_temp;

alter table ptestfilter_n0_temp drop partition (c=1, d=1);
show partitions ptestfilter_n0_temp;

alter table ptestfilter_n0_temp drop partition (c=2);
show partitions ptestfilter_n0_temp;

drop table ptestfilter_n0_temp;


