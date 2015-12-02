set hive.mapred.mode=nonstrict;
set hive.auto.convert.join=true;

-- SORT_QUERY_RESULTS

CREATE TABLE src11 as SELECT * FROM src;
CREATE TABLE src12 as SELECT * FROM src;
CREATE TABLE src13 as SELECT * FROM src;
CREATE TABLE src14 as SELECT * FROM src;


EXPLAIN SELECT * FROM 
src11 a JOIN
src12 b ON (a.key = b.key) JOIN
(SELECT * FROM (SELECT * FROM src13 UNION ALL SELECT * FROM src14)a )c ON c.value = b.value;
