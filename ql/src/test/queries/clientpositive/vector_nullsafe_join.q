SET hive.vectorized.execution.enabled=true;
SET hive.auto.convert.join=true;

-- SORT_QUERY_RESULTS
--
-- We currently do not support null safes (i.e the <=> operator) in native vector map join.
-- The explain output will show vectorized execution for both.  We verify the query
-- results are the same (HIVE-10628 shows native will produce the wrong results
-- otherwise).
--
-- This query for "HIVE-3315 join predicate transitive" triggers HIVE-10640-
-- explain select * from myinput1 a join myinput1 b on a.key<=>b.value AND a.key is NULL;
-- select * from myinput1 a join myinput1 b on a.key<=>b.value AND a.key is NULL;
--
CREATE TABLE myinput1_txt(key int, value int);
LOAD DATA LOCAL INPATH '../../data/files/in8.txt' INTO TABLE myinput1_txt;
CREATE TABLE myinput1 STORED AS ORC AS SELECT * FROM myinput1_txt;

SET hive.vectorized.execution.mapjoin.native.enabled=false;

-- merging
explain select * from myinput1 a join myinput1 b on a.key<=>b.value;
select * from myinput1 a join myinput1 b on a.key<=>b.value;

explain select * from myinput1 a join myinput1 b on a.key<=>b.value join myinput1 c on a.key=c.key;
select * from myinput1 a join myinput1 b on a.key<=>b.value join myinput1 c on a.key=c.key;

explain select * from myinput1 a join myinput1 b on a.key<=>b.value join myinput1 c on a.key<=>c.key;
select * from myinput1 a join myinput1 b on a.key<=>b.value join myinput1 c on a.key<=>c.key;

explain select * from myinput1 a join myinput1 b on a.key<=>b.value AND a.value=b.key join myinput1 c on a.key<=>c.key AND a.value=c.value;
select * from myinput1 a join myinput1 b on a.key<=>b.value AND a.value=b.key join myinput1 c on a.key<=>c.key AND a.value=c.value;

explain select * from myinput1 a join myinput1 b on a.key<=>b.value AND a.value<=>b.key join myinput1 c on a.key<=>c.key AND a.value<=>c.value;
select * from myinput1 a join myinput1 b on a.key<=>b.value AND a.value<=>b.key join myinput1 c on a.key<=>c.key AND a.value<=>c.value;

-- outer joins
SELECT * FROM myinput1 a LEFT OUTER JOIN myinput1 b ON a.key<=>b.value;
SELECT * FROM myinput1 a RIGHT OUTER JOIN myinput1 b ON a.key<=>b.value;
SELECT * FROM myinput1 a FULL OUTER JOIN myinput1 b ON a.key<=>b.value;

-- map joins
SELECT /*+ MAPJOIN(a) */ * FROM myinput1 a JOIN myinput1 b ON a.key<=>b.value;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1 a JOIN myinput1 b ON a.key<=>b.value;

SET hive.vectorized.execution.mapjoin.native.enabled=true;

-- merging
explain select * from myinput1 a join myinput1 b on a.key<=>b.value;
select * from myinput1 a join myinput1 b on a.key<=>b.value;

explain select * from myinput1 a join myinput1 b on a.key<=>b.value join myinput1 c on a.key=c.key;
select * from myinput1 a join myinput1 b on a.key<=>b.value join myinput1 c on a.key=c.key;

explain select * from myinput1 a join myinput1 b on a.key<=>b.value join myinput1 c on a.key<=>c.key;
select * from myinput1 a join myinput1 b on a.key<=>b.value join myinput1 c on a.key<=>c.key;

explain select * from myinput1 a join myinput1 b on a.key<=>b.value AND a.value=b.key join myinput1 c on a.key<=>c.key AND a.value=c.value;
select * from myinput1 a join myinput1 b on a.key<=>b.value AND a.value=b.key join myinput1 c on a.key<=>c.key AND a.value=c.value;

explain select * from myinput1 a join myinput1 b on a.key<=>b.value AND a.value<=>b.key join myinput1 c on a.key<=>c.key AND a.value<=>c.value;
select * from myinput1 a join myinput1 b on a.key<=>b.value AND a.value<=>b.key join myinput1 c on a.key<=>c.key AND a.value<=>c.value;

-- outer joins
SELECT * FROM myinput1 a LEFT OUTER JOIN myinput1 b ON a.key<=>b.value;
SELECT * FROM myinput1 a RIGHT OUTER JOIN myinput1 b ON a.key<=>b.value;
SELECT * FROM myinput1 a FULL OUTER JOIN myinput1 b ON a.key<=>b.value;

-- map joins
SELECT /*+ MAPJOIN(a) */ * FROM myinput1 a JOIN myinput1 b ON a.key<=>b.value;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1 a JOIN myinput1 b ON a.key<=>b.value;

