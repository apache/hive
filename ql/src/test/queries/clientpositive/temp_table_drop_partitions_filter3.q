set metastore.integral.jdo.pushdown=true;
create temporary table ptestfilter_n3_temp (a string, b int) partitioned by (c string, d int);
describe ptestfilter_n3_temp;

alter table ptestfilter_n3_temp add partition (c='1', d=1);
alter table ptestfilter_n3_temp add partition (c='1', d=2);
alter table ptestfilter_n3_temp add partition (c='2', d=1);
alter table ptestfilter_n3_temp add partition (c='2', d=2);
alter table ptestfilter_n3_temp add partition (c='3', d=1);
alter table ptestfilter_n3_temp add partition (c='3', d=2);
show partitions ptestfilter_n3_temp;

alter table ptestfilter_n3_temp drop partition (c='1', d=1);
show partitions ptestfilter_n3_temp;

alter table ptestfilter_n3_temp drop partition (c='2');
show partitions ptestfilter_n3_temp;

drop table ptestfilter_n3_temp;


