create table ptestfilter_n1 (a string, b int) partitioned by (c string, d string);
describe ptestfilter_n1;

alter table ptestfilter_n1 add partition (c='US', d=1);
alter table ptestfilter_n1 add partition (c='US', d=2);
alter table ptestFilter_n1 add partition (c='Uganda', d=2);
alter table ptestfilter_n1 add partition (c='Germany', d=2);
alter table ptestfilter_n1 add partition (c='Canada', d=3);
alter table ptestfilter_n1 add partition (c='Russia', d=3);
alter table ptestfilter_n1 add partition (c='Greece', d=2);
alter table ptestfilter_n1 add partition (c='India', d=3);
alter table ptestfilter_n1 add partition (c='France', d=4);
show partitions ptestfilter_n1;

alter table ptestfilter_n1 drop partition (c='US', d<'2');
show partitions ptestfilter_n1;

alter table ptestfilter_n1 drop partition (c>='US', d<='2');
show partitions ptestfilter_n1;

alter table ptestfilter_n1 drop partition (c >'India');
show partitions ptestfilter_n1;

alter table ptestfilter_n1 drop partition (c >='India'),
                             partition (c='Greece', d='2');
show partitions ptestfilter_n1;

alter table ptestfilter_n1 drop partition (c != 'France');
show partitions ptestfilter_n1;

set hive.exec.drop.ignorenonexistent=false;
alter table ptestfilter_n1 drop if exists partition (c='US');
show partitions ptestfilter_n1;

drop table ptestfilter_n1;


