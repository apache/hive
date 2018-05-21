set hive.mapred.mode=nonstrict;
set hive.cbo.returnpath.hiveop=true;
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

create table if not exists loc_staging_n1 (
  state string,
  locid int,
  zip bigint,
  year int
) row format delimited fields terminated by '|' stored as textfile;

create table loc_orc_n1 like loc_staging_n1;
alter table loc_orc_n1 set fileformat orc;

load data local inpath '../../data/files/loc.txt' overwrite into table loc_staging_n1;

insert overwrite table loc_orc_n1 select * from loc_staging_n1;

-- numRows: 8 rawDataSize: 796
explain select * from loc_orc_n1;

-- partial column stats
analyze table loc_orc_n1 compute statistics for columns state;

-- inner group by: map - numRows: 8 reduce - numRows: 4
-- outer group by: map - numRows: 4 reduce numRows: 2
explain select a, c, min(b)
from ( select state as a, locid as b, count(*) as c
       from loc_orc_n1
       group by state,locid
     ) sq1
group by a,c;

analyze table loc_orc_n1 compute statistics for columns state,locid,year;

-- Case 5: column stats, NO hash aggregation, NO grouping sets - cardinality = 8
-- Case 9: column stats, NO grouping sets - caridnality = 2
explain select year from loc_orc_n1 group by year;

-- Case 5: column stats, NO hash aggregation, NO grouping sets - cardinality = 8
-- Case 9: column stats, NO grouping sets - caridnality = 8
explain select state,locid from loc_orc_n1 group by state,locid;

-- Case 6: column stats, NO hash aggregation, grouping sets - cardinality = 32
-- Case 8: column stats, grouping sets - cardinality = 32
explain select state,locid from loc_orc_n1 group by state,locid with cube;

-- Case 6: column stats, NO hash aggregation, grouping sets - cardinality = 24
-- Case 8: column stats, grouping sets - cardinality = 24
explain select state,locid from loc_orc_n1 group by state,locid with rollup;
explain select state,locid from loc_orc_n1 group by rollup (state,locid);

-- Case 6: column stats, NO hash aggregation, grouping sets - cardinality = 8
-- Case 8: column stats, grouping sets - cardinality = 8
explain select state,locid from loc_orc_n1 group by state,locid grouping sets((state));

-- Case 6: column stats, NO hash aggregation, grouping sets - cardinality = 16
-- Case 8: column stats, grouping sets - cardinality = 16
explain select state,locid from loc_orc_n1 group by state,locid grouping sets((state),(locid));

-- Case 6: column stats, NO hash aggregation, grouping sets - cardinality = 24
-- Case 8: column stats, grouping sets - cardinality = 24
explain select state,locid from loc_orc_n1 group by state,locid grouping sets((state),(locid),());

-- Case 6: column stats, NO hash aggregation, grouping sets - cardinality = 32
-- Case 8: column stats, grouping sets - cardinality = 32
explain select state,locid from loc_orc_n1 group by state,locid grouping sets((state,locid),(state),(locid),());

set hive.map.aggr.hash.percentmemory=0.5f;
set mapred.max.split.size=80;
-- map-side parallelism will be 10

-- Case 3: column stats, hash aggregation, NO grouping sets - cardinality = 4
-- Case 9: column stats, NO grouping sets - caridnality = 2
explain select year from loc_orc_n1 group by year;

-- Case 4: column stats, hash aggregation, grouping sets - cardinality = 16
-- Case 8: column stats, grouping sets - cardinality = 16
explain select state,locid from loc_orc_n1 group by state,locid with cube;

-- ndvProduct becomes 0 as zip does not have column stats
-- Case 3: column stats, hash aggregation, NO grouping sets - cardinality = 4
-- Case 9: column stats, NO grouping sets - caridnality = 2
explain select state,zip from loc_orc_n1 group by state,zip;

set mapred.max.split.size=1000;
set hive.stats.fetch.column.stats=false;

-- Case 2: NO column stats, NO hash aggregation, NO grouping sets - cardinality = 32
-- Case 7: NO column stats - cardinality = 16
explain select state,locid from loc_orc_n1 group by state,locid with cube;

-- Case 2: NO column stats, NO hash aggregation, NO grouping sets - cardinality = 24
-- Case 7: NO column stats - cardinality = 12
explain select state,locid from loc_orc_n1 group by state,locid with rollup;
explain select state,locid from loc_orc_n1 group by rollup (state,locid);

-- Case 2: NO column stats, NO hash aggregation, NO grouping sets - cardinality = 8
-- Case 7: NO column stats - cardinality = 4
explain select state,locid from loc_orc_n1 group by state,locid grouping sets((state));

-- Case 2: NO column stats, NO hash aggregation, NO grouping sets - cardinality = 16
-- Case 7: NO column stats - cardinality = 8
explain select state,locid from loc_orc_n1 group by state,locid grouping sets((state),(locid));

-- Case 2: NO column stats, NO hash aggregation, NO grouping sets - cardinality = 24
-- Case 7: NO column stats - cardinality = 12
explain select state,locid from loc_orc_n1 group by state,locid grouping sets((state),(locid),());

-- Case 2: NO column stats, NO hash aggregation, NO grouping sets - cardinality = 32
-- Case 7: NO column stats - cardinality = 16
explain select state,locid from loc_orc_n1 group by state,locid grouping sets((state,locid),(state),(locid),());

set mapred.max.split.size=80;

-- Case 1: NO column stats, NO hash aggregation, NO grouping sets - cardinality = 8
-- Case 7: NO column stats - cardinality = 4
explain select year from loc_orc_n1 group by year;

-- Case 2: NO column stats, NO hash aggregation, NO grouping sets - cardinality = 32
-- Case 7: NO column stats - cardinality = 16
explain select state,locid from loc_orc_n1 group by state,locid with cube;

