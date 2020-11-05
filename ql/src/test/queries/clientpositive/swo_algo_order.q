--set hive.tez.dynamic.partition.pruning=false;

set hive.explain.user=true;
set hive.optimize.index.filter=true;
set hive.auto.convert.join=false;
set hive.vectorized.execution.enabled=true;

set hive.auto.convert.join.noconditionaltask.size=1;
set hive.tez.dynamic.partition.pruning=true;
set hive.tez.dynamic.semijoin.reduction=true;
set hive.optimize.index.filter=true;
set hive.tez.bigtable.minsize.semijoin.reduction=1;
set hive.tez.min.bloom.filter.entries=1;
set hive.tez.bloom.filter.factor=1.0f;

drop table if exists x1_store_sales;
drop table if exists x1_date_dim;
drop table if exists x1_item;

create table x1_store_sales 
(
	ss_item_sk	int,
	ss_sold_date_sk int
)
stored as orc;

create table x1_date_dim
(
	d_date_sk	int,
	d_month_seq	int,
	d_year		int,
	d_moy		int
)
stored as orc;


insert into x1_date_dim values	(1,1,2000,2),
				(2,2,2001,2);
insert into x1_store_sales values (1,1);
insert into x1_store_sales values (2,2);

alter table x1_store_sales update statistics set(
'numRows'='123456',
'rawDataSize'='1234567');

alter table x1_date_dim update statistics set(
'numRows'='56',
'rawDataSize'='81449');


select 'expected output here is to have 1 merged TS on SS while we still have 2 DD scans';
explain 
select   count(*) cnt
 from
     x1_store_sales s
     ,x1_date_dim d
 where  
	1=1
	and s.ss_sold_date_sk = d.d_date_sk
	and d.d_year=2000
union
select   s.ss_item_sk*d_date_sk
 from
     x1_store_sales s
     ,x1_date_dim d
 where  
	1=1
	and s.ss_sold_date_sk = d.d_date_sk
	and d.d_year=2001
	group by s.ss_item_sk*d_date_sk;

select   count(*) cnt
 from
     x1_store_sales s
     ,x1_date_dim d
 where  
	1=1
	and s.ss_sold_date_sk = d.d_date_sk
	and d.d_year=2000
union
select   s.ss_item_sk*d_date_sk
 from
     x1_store_sales s
     ,x1_date_dim d
 where  
	1=1
	and s.ss_sold_date_sk = d.d_date_sk
	and d.d_year=2001
	group by s.ss_item_sk*d_date_sk;
