-- HIVE-23365: Put RS deduplication optimization under cost based decision
-- In every query in this file reduce deduplication is applicable. Nevertheless, depending on the statistics the
-- optimizer may decide to apply it or not.
set hive.optimize.reducededuplication=true;

create table person (id integer, name string, sex string, country int); -- id: 4B, name:20B, sex:1B, country:4B
-- Based on the stats below the deduplication should be applied when the remaining partition columns contain id or name,
-- Respectively when the remaining partition columns are sex and country the deduplication shouldn't apply.
alter table person update statistics set('numRows'='741000000', 'rawDataSize'='21489000000');
alter table person update statistics for column id set('numDVs'='741000000','numNulls'='0', 'highValue'='100000000','lowValue'='0');
alter table person update statistics for column name set('numDVs'='7410000','numNulls'='10000', 'maxColLen'='20', 'avgColLen'='10');
alter table person update statistics for column sex set('numDVs'='3','numNulls'='1000000', 'maxColLen'='1', 'avgColLen'='1');
alter table person update statistics for column country set('numDVs'='44','numNulls'='1000', 'highValue'='43', 'lowValue'='0');

-- The deduplication changes the parallelism when the partition columns of the two ReduceSinkOperators since the usual
-- merge replaces the partitioning columns of the child RS with the columns of the parent.
-- Currently it takes place in two cases:
-- Case I: Parent RS columns are more specific than those of the child RS, and child columns are assigned;

explain select name, count(id) from (select * from person distribute by name, id) Q0 group by name;
explain select country, count(id) from (select * from person distribute by country, id) Q1 group by country;
explain select sex, count(id) from (select * from person distribute by sex, id) Q2 group by sex;
explain select name, count(id) from (select id, name from person group by name, id) Q3 group by name;
explain select country, count(id) from (select id, country from person group by id, country) Q4 group by country;
explain select sex, count(id) from (select id, sex from person group by id, sex) Q5 group by sex;

-- Case II: Child RS columns are more specific than those of the parent RS, and parent columns are not assigned.

set hive.remove.orderby.in.subquery=false;
explain select name, count(id) from (select * from person sort by name, id) Q7 group by name;
explain select country, count(id) from (select * from person sort by country, id) Q8 group by country;
explain select sex, count(id) from (select * from person sort by sex, id) Q9 group by sex;