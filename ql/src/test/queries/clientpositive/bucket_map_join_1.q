set hive.strict.checks.bucketing=false;

drop table table1_n9;
drop table table2_n5;

;


create table table1_n9(key string, value string) clustered by (key, value)
sorted by (key, value) into 1 BUCKETS stored as textfile;
create table table2_n5(key string, value string) clustered by (value, key)
sorted by (value, key) into 1 BUCKETS stored as textfile;

load data local inpath '../../data/files/SortCol1Col2/000000_0' overwrite into table table1_n9;
load data local inpath '../../data/files/SortCol2Col1/000000_0' overwrite into table table2_n5;

set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.cbo.enable=false;
-- The tables are bucketed in same columns in different order,
-- but sorted in different column orders
-- Neither bucketed map-join, nor sort-merge join should be performed

explain extended
select /*+ mapjoin(b) */ count(*) from table1_n9 a join table2_n5 b on a.key=b.key and a.value=b.value;

select /*+ mapjoin(b) */ count(*) from table1_n9 a join table2_n5 b on a.key=b.key and a.value=b.value;

