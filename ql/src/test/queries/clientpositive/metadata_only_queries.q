set hive.stats.column.autogather=false;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.compute.query.using.stats=true;
set hive.stats.autogather=true;
create table over10k_n12(
           t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
           ts timestamp, 
           `dec` decimal,  
           bin binary)
       row format delimited
       fields terminated by '|'
       TBLPROPERTIES ("hive.serialization.decode.binary.as.base64"="false");

load data local inpath '../../data/files/over10k' into table over10k_n12;

create table stats_tbl(
           t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
           ts timestamp,
           `dec` decimal,  
           bin binary);

create table stats_tbl_part(
           t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
           ts timestamp,
           `dec` decimal,  
           bin binary) partitioned by (dt string);


insert overwrite table stats_tbl select * from over10k_n12;

insert into table stats_tbl_part partition (dt='2010') select * from over10k_n12 where t>0 and t<30;
insert into table stats_tbl_part partition (dt='2011') select * from over10k_n12 where t>30 and t<60;
insert into table stats_tbl_part partition (dt='2012') select * from over10k_n12 where t>60;

explain 
select count(*), sum(1), sum(0.2), count(1), count(s), count(bo), count(bin), count(si), max(i), min(b) from stats_tbl;
explain
select count(*), sum(1), sum(0.2), count(1), count(s), count(bo), count(bin), count(si), max(i), min(b) from stats_tbl_part;

explain 
select count(*), '1' as one, sum(1), sum(0.2), 2 as two, count(1), count(s), 3+4.0 as three, count(bo), count(bin), count(si), max(i), min(b) from stats_tbl;
explain
select count(*), '1' as one, sum(1), sum(0.2), 2 as two, count(1), count(s), 3+4.0 as three, count(bo), count(bin), count(si), max(i), min(b) from stats_tbl_part;

analyze table stats_tbl compute statistics for columns t,si,i,b,f,d,`dec`,bo,s,bin;
analyze table stats_tbl_part partition(dt='2010') compute statistics for columns t,si,i,b,f,d,`dec`,bo,s,bin;
analyze table stats_tbl_part partition(dt='2011') compute statistics for columns t,si,i,b,f,d,`dec`,bo,s,bin;
analyze table stats_tbl_part partition(dt='2012') compute statistics for columns t,si,i,b,f,d,`dec`,bo,s,bin;

explain 
select count(*), sum(1), sum(0.2), count(1), count(s), count(bo), count(bin), count(si) from stats_tbl;
select count(*), sum(1), sum(0.2), count(1), count(s), count(bo), count(bin), count(si) from stats_tbl;
explain
select min(i), max(i), min(b), max(b), min(f), max(f), min(d), max(d) from stats_tbl;
select min(i), max(i), min(b), max(b), min(f), max(f), min(d), max(d) from stats_tbl;

explain
select min(i), '1' as one, max(i), min(b), max(b), min(f), max(f), 3+4.0 as three, min(d), max(d) from stats_tbl;
select min(i), '1' as one, max(i), min(b), max(b), min(f), max(f), 3+4.0 as three, min(d), max(d) from stats_tbl;



explain 
select count(*), sum(1), sum(0.2), count(1), count(s), count(bo), count(bin), count(si) from stats_tbl_part;
select count(*), sum(1), sum(0.2), count(1), count(s), count(bo), count(bin), count(si) from stats_tbl_part;
explain
select min(i), max(i), min(b), max(b), min(f), max(f), min(d), max(d) from stats_tbl_part;
select min(i), max(i), min(b), max(b), min(f), max(f), min(d), max(d) from stats_tbl_part;

explain
select min(i), '1' as one, max(i), min(b), max(b), min(f), max(f), 3+4.0 as three, min(d), max(d) from stats_tbl_part;
select min(i), '1' as one, max(i), min(b), max(b), min(f), max(f), 3+4.0 as three, min(d), max(d) from stats_tbl_part;

-- HIVE-30131: a decimal column is answered from its statistics like any other type
explain
select min(`dec`), max(`dec`), count(`dec`) from stats_tbl;
select min(`dec`), max(`dec`), count(`dec`) from stats_tbl;

explain
select min(`dec`), max(`dec`), count(`dec`) from stats_tbl_part;
select min(`dec`), max(`dec`), count(`dec`) from stats_tbl_part;

-- the bounds a decimal column reports, including a column that is entirely NULL. 40.60 is stored
-- normalised to scale 1, so reading it back padded to the column's scale exercises the object
-- inspector rather than just the statistics.
create table stats_dec(d decimal(5,2), n decimal(7,3));
insert into stats_dec values (-10.40, null), (40.60, null), (1.50, null), (null, null);
analyze table stats_dec compute statistics for columns d,n;

explain
select min(d), max(d), count(d), min(n), max(n), count(n) from stats_dec;
select min(d), max(d), count(d), min(n), max(n), count(n) from stats_dec;

-- and over partitions, where each bound is the aggregate of the per-partition statistics
create table stats_dec_part(d decimal(5,2)) partitioned by (p string);
insert into stats_dec_part partition(p='a') values (-10.40), (1.50);
insert into stats_dec_part partition(p='b') values (40.60), (null);
analyze table stats_dec_part partition(p) compute statistics for columns d;

explain
select min(d), max(d), count(d) from stats_dec_part;
select min(d), max(d), count(d) from stats_dec_part;

explain select count(ts) from stats_tbl_part;

explain select count('1') from stats_tbl group by '1';
select count('1') from stats_tbl group by '1';

explain select count('1') from stats_tbl_part group by '1';
select count('1') from stats_tbl_part group by '1';

drop table stats_tbl;
drop table stats_tbl_part;

set hive.compute.query.using.stats=false;
