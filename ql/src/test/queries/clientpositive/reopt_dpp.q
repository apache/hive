set hive.explain.user=true;
set hive.optimize.index.filter=true;
set hive.auto.convert.join=true;
set hive.vectorized.execution.enabled=true;

drop table if exists x1_store_sales;
drop table if exists x1_date_dim;
drop table if exists x1_item;

create table x1_store_sales 
(
	ss_item_sk	int
)
partitioned by (ss_sold_date_sk int)
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
insert into x1_store_sales partition (ss_sold_date_sk=1) values (1);
insert into x1_store_sales partition (ss_sold_date_sk=2) values (2);

alter table x1_store_sales partition (ss_sold_date_sk=1) update statistics set(
'numRows'='123456',
'rawDataSize'='1234567');

alter table x1_date_dim update statistics set(
'numRows'='56',
'rawDataSize'='81449');


-- the following query is designed to produce a DPP plan
explain 
select   count(*) cnt
 from
     x1_store_sales s
     ,x1_date_dim d
 where  
	1=1
	and s.ss_sold_date_sk = d.d_date_sk
	and d.d_year=2000;

-- tablescan of s should be 2 or 123456 rows; but never 1
-- and it should not be a mapjoin :)
explain reoptimization
select   count(*) cnt
 from
     x1_store_sales s
     ,x1_date_dim d
 where  
	1=1
	and s.ss_sold_date_sk = d.d_date_sk
	and d.d_year=2000;
