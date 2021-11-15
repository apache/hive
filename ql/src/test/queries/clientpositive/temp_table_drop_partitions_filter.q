create temporary table ptestfilter_n1_temp (a string, b int) partitioned by (c string, d string);
describe ptestfilter_n1_temp;

explain alter table ptestfilter_n1_temp add partition (c='US', d=1);
alter table ptestfilter_n1_temp add partition (c='US', d=1);
alter table ptestfilter_n1_temp add partition (c='US', d=2);
alter table ptestfilter_n1_temp add partition (c='Uganda', d=2);
alter table ptestfilter_n1_temp add partition (c='Germany', d=2);
alter table ptestfilter_n1_temp add partition (c='Canada', d=3);
alter table ptestfilter_n1_temp add partition (c='Russia', d=3);
alter table ptestfilter_n1_temp add partition (c='Greece', d=2);
alter table ptestfilter_n1_temp add partition (c='India', d=3);
alter table ptestfilter_n1_temp add partition (c='France', d=4);
show partitions ptestfilter_n1_temp;

explain alter table ptestfilter_n1_temp drop partition (c='US', d<'2');
alter table ptestfilter_n1_temp drop partition (c='US', d<'2');
explain show partitions ptestfilter_n1_temp;
show partitions ptestfilter_n1_temp;

alter table ptestfilter_n1_temp drop partition (c>='US', d<='2');
show partitions ptestfilter_n1_temp;

alter table ptestfilter_n1_temp drop partition (c >'India');
show partitions ptestfilter_n1_temp;

explain alter table ptestfilter_n1_temp drop partition (c >='India'),
partition (c='Greece', d='2');
alter table ptestfilter_n1_temp drop partition (c >='India'),
partition (c='Greece', d='2');
show partitions ptestfilter_n1_temp;

alter table ptestfilter_n1_temp drop partition (c != 'France');
show partitions ptestfilter_n1_temp;

drop table ptestfilter_n1_temp;


