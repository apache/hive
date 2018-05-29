--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.auto.convert.join = true;

create table tst1_n2(key STRING, cnt INT);

INSERT OVERWRITE TABLE tst1_n2
SELECT a.key, count(1) FROM src a group by a.key;

explain 
SELECT sum(a.cnt)  FROM tst1_n2 a JOIN tst1_n2 b ON a.key = b.key;

SELECT sum(a.cnt)  FROM tst1_n2 a JOIN tst1_n2 b ON a.key = b.key;


