-- HIVE-4342
-- Maponly union(UNION-13) is merged into non-maponly union(UNION-15)
-- In this case, task for UNION-13 should be removed from top-task and merged into task for UNION-15
-- TS[2]-SEL[3]-RS[5]-JOIN[6]-SEL[7]-UNION[15]-SEL[16]-RS[17]-EX[18]-FS[19]
-- TS[0]-SEL[1]-RS[4]-JOIN[6]
-- TS[8]-SEL[9]-UNION[13]-SEL[14]-UNION[15]
-- TS[11]-SEL[12]-UNION[13]

create table src10_1 (key string, value string);
create table src10_2 (key string, value string);
create table src10_3 (key string, value string);
create table src10_4 (key string, value string);

from (select * from src limit 10) a
insert overwrite table src10_1 select *
insert overwrite table src10_2 select *
insert overwrite table src10_3 select *
insert overwrite table src10_4 select *;

set hive.auto.convert.join=true;

explain
SELECT * FROM (
  SELECT sub1.key,sub1.value FROM (SELECT * FROM src10_1) sub1 JOIN (SELECT * FROM src10_2) sub0 ON (sub0.key = sub1.key)
  UNION ALL
  SELECT key,value FROM (SELECT * FROM (SELECT * FROM src10_3) sub2 UNION ALL SELECT * FROM src10_4 ) alias0
) alias1 order by key;

SELECT * FROM (
  SELECT sub1.key,sub1.value FROM (SELECT * FROM src10_1) sub1 JOIN (SELECT * FROM src10_2) sub0 ON (sub0.key = sub1.key)
  UNION ALL
  SELECT key,value FROM (SELECT * FROM (SELECT * FROM src10_3) sub2 UNION ALL SELECT * FROM src10_4 ) alias0
) alias1 order by key;

set hive.auto.convert.join=false;

explain
SELECT * FROM (
  SELECT sub1.key,sub1.value FROM (SELECT * FROM src10_1) sub1 JOIN (SELECT * FROM src10_2) sub0 ON (sub0.key = sub1.key)
  UNION ALL
  SELECT key,value FROM (SELECT * FROM (SELECT * FROM src10_3) sub2 UNION ALL SELECT * FROM src10_4 ) alias0
) alias1 order by key;

SELECT * FROM (
  SELECT sub1.key,sub1.value FROM (SELECT * FROM src10_1) sub1 JOIN (SELECT * FROM src10_2) sub0 ON (sub0.key = sub1.key)
  UNION ALL
  SELECT key,value FROM (SELECT * FROM (SELECT * FROM src10_3) sub2 UNION ALL SELECT * FROM src10_4 ) alias0
) alias1 order by key;
