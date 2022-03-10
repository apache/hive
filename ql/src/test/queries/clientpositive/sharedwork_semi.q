set hive.explain.user=true;
set hive.optimize.index.filter=true;
set hive.auto.convert.join=true;
set hive.vectorized.execution.enabled=true;
set hive.optimize.shared.work.semijoin=true;

drop table if exists x1_store_sales;
drop table if exists x1_date_dim;

create table x1_store_sales 
(
	ss_sold_date_sk int,
	ss_item_sk	int
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

insert into x1_date_dim values	(1,1,2000,1),
				(2,2,2001,2),
				(3,2,2001,3),
				(4,2,2001,4),
				(5,2,2001,5),
				(6,2,2001,6),
				(7,2,2001,7),
				(8,2,2001,8);

insert into x1_store_sales values (1,1),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10),(11,11);

alter table x1_store_sales update statistics set(
'numRows'='123456',
'rawDataSize'='1234567');

alter table x1_date_dim update statistics set(
'numRows'='28',
'rawDataSize'='81449');


set hive.auto.convert.join.noconditionaltask.size=1;
set hive.tez.dynamic.partition.pruning=true;
set hive.tez.dynamic.semijoin.reduction=true;
set hive.optimize.index.filter=true;
set hive.tez.bigtable.minsize.semijoin.reduction=1;
set hive.tez.min.bloom.filter.entries=1;
set hive.tez.bloom.filter.factor=1.0f;
set hive.explain.user=false;

set hive.optimize.shared.work.dppunion=false;

select 'expected to see a plan in which ts scan could be shared by combining semijoin conditions';
-- note: this plan should involve a semijoin reduction
explain 
select   sum(s.ss_item_sk)
 from
     x1_store_sales s
     ,x1_date_dim d
 where
        1=1
        and s.ss_sold_date_sk=d.d_date_sk
	and d.d_moy=3
union
select   sum(s.ss_item_sk)
 from
     x1_store_sales s
     ,x1_date_dim d
 where
        1=1
        and s.ss_sold_date_sk=d.d_date_sk
	and d.d_moy=5
;

select   sum(s.ss_item_sk)
 from
     x1_store_sales s
     ,x1_date_dim d
 where
        1=1
        and s.ss_sold_date_sk=d.d_date_sk
	and d.d_moy=3
union
select   sum(s.ss_item_sk)
 from
     x1_store_sales s
     ,x1_date_dim d
 where
        1=1
        and s.ss_sold_date_sk=d.d_date_sk
	and d.d_moy=5
;


set hive.optimize.shared.work.dppunion=true;

select 'expected to see a plan in which x1_store_sales(s) is only scanned once';
explain 
select   sum(s.ss_item_sk)
 from
     x1_store_sales s
     ,x1_date_dim d
 where
        1=1
        and s.ss_sold_date_sk=d.d_date_sk
	and d.d_moy=3
union
select   sum(s.ss_item_sk)
 from
     x1_store_sales s
     ,x1_date_dim d
 where
        1=1
        and s.ss_sold_date_sk=d.d_date_sk
	and d.d_moy=5
;

select   sum(s.ss_item_sk)
 from
     x1_store_sales s
     ,x1_date_dim d
 where
        1=1
        and s.ss_sold_date_sk=d.d_date_sk
	and d.d_moy=3
union
select   sum(s.ss_item_sk)
 from
     x1_store_sales s
     ,x1_date_dim d
 where
        1=1
        and s.ss_sold_date_sk=d.d_date_sk
	and d.d_moy=5
;

