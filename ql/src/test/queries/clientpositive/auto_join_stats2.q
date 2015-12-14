set hive.mapred.mode=nonstrict;
set hive.auto.convert.join = true;

-- Auto_join2 no longer tests merging the mapjoin work if big-table selection is based on stats, as src3 is smaller statistically than src1 + src2.
-- Hence forcing the third table to be smaller.

create table smalltable(key string, value string) stored as textfile;
load data local inpath '../../data/files/T1.txt' into table smalltable;

explain select src1.key, src2.key, smalltable.key from src src1 JOIN src src2 ON (src1.key = src2.key) JOIN smalltable ON (src1.key + src2.key = smalltable.key);
select src1.key, src2.key, smalltable.key from src src1 JOIN src src2 ON (src1.key = src2.key) JOIN smalltable ON (src1.key + src2.key = smalltable.key);

create table smalltable2(key string, value string) stored as textfile;
load data local inpath '../../data/files/T1.txt' into table smalltable2;
analyze table smalltable compute statistics;

explain select src1.key, src2.key, smalltable.key from src src1 JOIN src src2 ON (src1.key = src2.key) JOIN smalltable ON (src1.key + src2.key = smalltable.key) JOIN smalltable2 ON (src1.key + src2.key = smalltable2.key);
select src1.key, src2.key, smalltable.key from src src1 JOIN src src2 ON (src1.key = src2.key) JOIN smalltable ON (src1.key + src2.key = smalltable.key) JOIN smalltable2 ON (src1.key + src2.key = smalltable2.key);