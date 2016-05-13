set hive.mapred.mode=nonstrict;

drop table part_hive;
drop table partsupp_hive;
drop table supplier_hive;

create table part_hive (P_PARTKEY INT, P_NAME STRING, P_MFGR STRING, P_BRAND STRING, P_TYPE STRING, 
P_SIZE INT, P_CONTAINER STRING, P_RETAILPRICE DOUBLE, P_COMMENT STRING);

create table partsupp_hive (PS_PARTKEY INT, PS_SUPPKEY INT, PS_AVAILQTY INT, PS_SUPPLYCOST DOUBLE, 
PS_COMMENT STRING);

create table supplier_hive (S_SUPPKEY INT, S_NAME STRING, S_ADDRESS STRING, S_NATIONKEY INT, 
S_PHONE STRING, S_ACCTBAL DOUBLE, S_COMMENT STRING);

analyze table part_hive compute statistics;
analyze table part_hive compute statistics for columns;

analyze table partsupp_hive compute statistics;
analyze table partsupp_hive compute statistics for columns;

analyze table supplier_hive compute statistics;
analyze table supplier_hive compute statistics for columns;

explain select
	p_brand,
	p_type,
	p_size,
	count(distinct ps_suppkey) as supplier_cnt
from
	partsupp_hive,
	part_hive
where
	p_partkey = ps_partkey
	and p_brand <> 'Brand#34'
	and p_type not like 'ECONOMY BRUSHED%'
	and p_size in (22, 14, 27, 49, 21, 33, 35, 28)
	and partsupp_hive.ps_suppkey not in (
		select
			s_suppkey
		from
			supplier_hive
		where
			s_comment like '%Customer%Complaints%'
	)
group by
	p_brand,
	p_type,
	p_size
order by
	supplier_cnt desc,
	p_brand,
	p_type,
	p_size;
