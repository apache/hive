set hive.exec.dynamic.partition.mode=nonstrict;
set hive.stats.fetch.column.stats=true;

create table date_dim_n1 (d_date date) partitioned by (d_date_sk bigint) stored as orc;
insert into date_dim_n1 partition(d_date_sk=2416945) values('1905-04-09');
insert into date_dim_n1 partition(d_date_sk=2416946) values('1905-04-10');
insert into date_dim_n1 partition(d_date_sk=2416947) values('1905-04-11');
analyze table date_dim_n1 partition(d_date_sk) compute statistics for columns;

explain select count(*) from date_dim_n1 where d_date > date "1900-01-02" and d_date_sk= 2416945;

insert into date_dim_n1 partition(d_date_sk=2416948) values('1905-04-12');

explain extended select d_date from date_dim_n1;
