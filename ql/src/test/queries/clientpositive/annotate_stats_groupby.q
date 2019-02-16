set hive.stats.fetch.column.stats=true;
set hive.map.aggr.hash.percentmemory=0.0f;

-- hash aggregation is disabled

-- There are different cases for Group By depending on map/reduce side, hash aggregation,
-- grouping sets and column stats. If we don't have column stats, we just assume hash
-- aggregation is disabled. Following are the possible cases and rule for cardinality
-- estimation

-- MAP SIDE:
-- Case 1: NO column stats, NO hash aggregation, NO grouping sets — numRows
-- Case 2: NO column stats, NO hash aggregation, grouping sets — numRows * sizeOfGroupingSet
-- Case 3: column stats, hash aggregation, NO grouping sets — Min(numRows / 2, ndvProduct * parallelism)
-- Case 4: column stats, hash aggregation, grouping sets — Min((numRows * sizeOfGroupingSet) / 2, ndvProduct * parallelism * sizeOfGroupingSet)
-- Case 5: column stats, NO hash aggregation, NO grouping sets — numRows
-- Case 6: column stats, NO hash aggregation, grouping sets — numRows * sizeOfGroupingSet

-- REDUCE SIDE:
-- Case 7: NO column stats — numRows / 2
-- Case 8: column stats, grouping sets — Min(numRows, ndvProduct * sizeOfGroupingSet)
-- Case 9: column stats, NO grouping sets - Min(numRows, ndvProduct)

create table if not exists loc_staging_n2 (
  state string,
  locid int,
  zip bigint,
  year int
) row format delimited fields terminated by '|' stored as textfile;

create table loc_orc_n2 like loc_staging_n2;
alter table loc_orc_n2 set fileformat orc;

load data local inpath '../../data/files/loc.txt' overwrite into table loc_staging_n2;

insert overwrite table loc_orc_n2 select * from loc_staging_n2;

-- numRows: 8 rawDataSize: 796
explain select * from loc_orc_n2;

-- partial column stats
analyze table loc_orc_n2 compute statistics for columns state;

-- inner group by: map - numRows: 8 reduce - numRows: 4
-- outer group by: map - numRows: 4 reduce numRows: 2
explain select a, c, min(b)
from ( select state as a, locid as b, count(*) as c
       from loc_orc_n2
       group by state,locid
     ) sq1
group by a,c;

analyze table loc_orc_n2 compute statistics for columns state,locid,year;

-- Case 5: column stats, NO hash aggregation, NO grouping sets - cardinality = 8
-- Case 9: column stats, NO grouping sets - caridnality = 2
explain select year from loc_orc_n2 group by year;

-- Case 5: column stats, NO hash aggregation, NO grouping sets - cardinality = 8
-- Case 9: column stats, NO grouping sets - caridnality = 8
explain select state,locid from loc_orc_n2 group by state,locid;

-- Case 6: column stats, NO hash aggregation, grouping sets - cardinality = 32
-- Case 8: column stats, grouping sets - cardinality = 32
explain select state,locid from loc_orc_n2 group by state,locid with cube;

-- Case 6: column stats, NO hash aggregation, grouping sets - cardinality = 24
-- Case 8: column stats, grouping sets - cardinality = 24
explain select state,locid from loc_orc_n2 group by state,locid with rollup;
explain select state,locid from loc_orc_n2 group by rollup( state,locid );

-- Case 6: column stats, NO hash aggregation, grouping sets - cardinality = 8
-- Case 8: column stats, grouping sets - cardinality = 8
explain select state,locid from loc_orc_n2 group by state,locid grouping sets((state));

-- Case 6: column stats, NO hash aggregation, grouping sets - cardinality = 16
-- Case 8: column stats, grouping sets - cardinality = 16
explain select state,locid from loc_orc_n2 group by state,locid grouping sets((state),(locid));

-- Case 6: column stats, NO hash aggregation, grouping sets - cardinality = 24
-- Case 8: column stats, grouping sets - cardinality = 24
explain select state,locid from loc_orc_n2 group by state,locid grouping sets((state),(locid),());

-- Case 6: column stats, NO hash aggregation, grouping sets - cardinality = 32
-- Case 8: column stats, grouping sets - cardinality = 32
explain select state,locid from loc_orc_n2 group by state,locid grouping sets((state,locid),(state),(locid),());

set hive.map.aggr.hash.percentmemory=0.5f;
set mapred.max.split.size=80;
-- map-side parallelism will be 10

-- Case 3: column stats, hash aggregation, NO grouping sets - cardinality = 4
-- Case 9: column stats, NO grouping sets - caridnality = 2
explain select year from loc_orc_n2 group by year;

-- Case 4: column stats, hash aggregation, grouping sets - cardinality = 16
-- Case 8: column stats, grouping sets - cardinality = 16
explain select state,locid from loc_orc_n2 group by state,locid with cube;

-- ndvProduct becomes 0 as zip does not have column stats
-- Case 3: column stats, hash aggregation, NO grouping sets - cardinality = 4
-- Case 9: column stats, NO grouping sets - caridnality = 2
explain select state,zip from loc_orc_n2 group by state,zip;

set mapred.max.split.size=1000;
set hive.stats.fetch.column.stats=false;

-- Case 2: NO column stats, NO hash aggregation, NO grouping sets - cardinality = 32
-- Case 7: NO column stats - cardinality = 16
explain select state,locid from loc_orc_n2 group by state,locid with cube;

-- Case 2: NO column stats, NO hash aggregation, NO grouping sets - cardinality = 24
-- Case 7: NO column stats - cardinality = 12
explain select state,locid from loc_orc_n2 group by state,locid with rollup;
explain select state,locid from loc_orc_n2 group by rollup (state,locid);

-- Case 2: NO column stats, NO hash aggregation, NO grouping sets - cardinality = 8
-- Case 7: NO column stats - cardinality = 4
explain select state,locid from loc_orc_n2 group by state,locid grouping sets((state));

-- Case 2: NO column stats, NO hash aggregation, NO grouping sets - cardinality = 16
-- Case 7: NO column stats - cardinality = 8
explain select state,locid from loc_orc_n2 group by state,locid grouping sets((state),(locid));

-- Case 2: NO column stats, NO hash aggregation, NO grouping sets - cardinality = 24
-- Case 7: NO column stats - cardinality = 12
explain select state,locid from loc_orc_n2 group by state,locid grouping sets((state),(locid),());

-- Case 2: NO column stats, NO hash aggregation, NO grouping sets - cardinality = 32
-- Case 7: NO column stats - cardinality = 16
explain select state,locid from loc_orc_n2 group by state,locid grouping sets((state,locid),(state),(locid),());

set mapred.max.split.size=80;

-- Case 1: NO column stats, NO hash aggregation, NO grouping sets - cardinality = 8
-- Case 7: NO column stats - cardinality = 4
explain select year from loc_orc_n2 group by year;

-- Case 2: NO column stats, NO hash aggregation, NO grouping sets - cardinality = 32
-- Case 7: NO column stats - cardinality = 16
explain select state,locid from loc_orc_n2 group by state,locid with cube;

set hive.stats.fetch.column.stats=true;

create table t1_uq12(i int, j int);
alter table t1_uq12 update statistics set('numRows'='10000', 'rawDataSize'='18000');
alter table t1_uq12 update statistics for column i set('numDVs'='2500','numNulls'='50','highValue'='1000','lowValue'='0');
alter table t1_uq12 update statistics for column j set('numDVs'='500','numNulls'='30','highValue'='100','lowValue'='50');

create table t2_uq12(i2 int, j2 int);
alter table t2_uq12 update statistics set('numRows'='100000000', 'rawDataSize'='10000');
alter table t2_uq12 update statistics for column i2 set('numDVs'='10000000','numNulls'='0','highValue'='8000','lowValue'='0');
alter table t2_uq12 update statistics for column j2 set('numDVs'='10','numNulls'='0','highValue'='800','lowValue'='-1');

explain select count (1) from t1_uq12,t2_uq12 where t1_uq12.j=t2_uq12.i2 group by t1_uq12.i, t1_uq12.j;

drop table t1_uq12;
drop table t2_uq12;

