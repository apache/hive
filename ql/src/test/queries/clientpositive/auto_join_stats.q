set hive.auto.convert.join = true;
set hive.auto.convert.join.noconditionaltask.size=2660;

-- Setting HTS(src2) < threshold < HTS(src2) + HTS(smalltable).
-- This query plan should thus not try to combine the mapjoin into a single work.

create table smalltable(key string, value string) stored as textfile;
load data local inpath '../../data/files/T1.txt' into table smalltable;
analyze table smalltable compute statistics;

explain select src1.key, src2.key, smalltable.key from src src1 JOIN src src2 ON (src1.key = src2.key) JOIN smalltable ON (src1.key + src2.key = smalltable.key);
select src1.key, src2.key, smalltable.key from src src1 JOIN src src2 ON (src1.key = src2.key) JOIN smalltable ON (src1.key + src2.key = smalltable.key);

create table smalltable2(key string, value string) stored as textfile;
load data local inpath '../../data/files/T1.txt' into table smalltable2;
analyze table smalltable compute statistics;

explain select src1.key, src2.key, smalltable.key from src src1 JOIN src src2 ON (src1.key = src2.key) JOIN smalltable ON (src1.key + src2.key = smalltable.key) JOIN smalltable2 ON (src1.key + src2.key = smalltable2.key);
select src1.key, src2.key, smalltable.key from src src1 JOIN src src2 ON (src1.key = src2.key) JOIN smalltable ON (src1.key + src2.key = smalltable.key) JOIN smalltable2 ON (src1.key + src2.key = smalltable2.key);