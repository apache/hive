drop table location;

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

create table location (state string, country string, votes bigint);
load data local inpath "../../data/files/location.txt" overwrite into table location;

analyze table location compute statistics;
analyze table location compute statistics for columns state, country;

set mapred.max.split.size=50;
set hive.map.aggr.hash.percentmemory=0.5f;
set hive.stats.fetch.column.stats=false;

-- Case 1: NO column stats, NO hash aggregation, NO grouping sets - cardinality = 20
-- Case 7: NO column stats - cardinality = 10
explain select state, country from location group by state, country;

-- Case 2: NO column stats, NO hash aggregation, NO grouping sets - cardinality = 80
-- Case 7: NO column stats - cardinality = 40
explain select state, country from location group by state, country with cube;

set hive.stats.fetch.column.stats=true;
-- parallelism = 4

-- Case 3: column stats, hash aggregation, NO grouping sets - cardinality = 8
-- Case 9: column stats, NO grouping sets - caridnality = 2
explain select state, country from location group by state, country;

-- column stats for votes is missing, so ndvProduct becomes 0 and will be set to numRows / 2
-- Case 3: column stats, hash aggregation, NO grouping sets - cardinality = 10
-- Case 9: column stats, NO grouping sets - caridnality = 5
explain select state, votes from location group by state, votes;

-- Case 4: column stats, hash aggregation, grouping sets - cardinality = 32
-- Case 8: column stats, grouping sets - cardinality = 8
explain select state, country from location group by state, country with cube;

set hive.map.aggr.hash.percentmemory=0.0f;
-- Case 5: column stats, NO hash aggregation, NO grouping sets - cardinality = 20
-- Case 9: column stats, NO grouping sets - caridnality = 2
explain select state, country from location group by state, country;

-- Case 6: column stats, NO hash aggregation, grouping sets - cardinality = 80
-- Case 8: column stats, grouping sets - cardinality = 8
explain select state, country from location group by state, country with cube;

drop table location;
