--! qt:dataset:src1
--! qt:dataset:src
set hive.auto.convert.join = true;
set hive.auto.convert.join.noconditionaltask.size=2660;

-- Setting HTS(src2) < threshold < HTS(src2) + HTS(smalltable_n0).
-- This query plan should thus not try to combine the mapjoin into a single work.

create table smalltable_n0(key string, value string) stored as textfile;
load data local inpath '../../data/files/T1.txt' into table smalltable_n0;
analyze table smalltable_n0 compute statistics;

explain select src1.key, src2.key, smalltable_n0.key from src src1 JOIN src src2 ON (src1.key = src2.key) JOIN smalltable_n0 ON (src1.key + src2.key = smalltable_n0.key);
select src1.key, src2.key, smalltable_n0.key from src src1 JOIN src src2 ON (src1.key = src2.key) JOIN smalltable_n0 ON (src1.key + src2.key = smalltable_n0.key);

create table smalltable2_n0(key string, value string) stored as textfile;
load data local inpath '../../data/files/T1.txt' into table smalltable2_n0;
analyze table smalltable_n0 compute statistics;

explain select src1.key, src2.key, smalltable_n0.key from src src1 JOIN src src2 ON (src1.key = src2.key) JOIN smalltable_n0 ON (src1.key + src2.key = smalltable_n0.key) JOIN smalltable2_n0 ON (src1.key + src2.key = smalltable2_n0.key);
select src1.key, src2.key, smalltable_n0.key from src src1 JOIN src src2 ON (src1.key = src2.key) JOIN smalltable_n0 ON (src1.key + src2.key = smalltable_n0.key) JOIN smalltable2_n0 ON (src1.key + src2.key = smalltable2_n0.key);