set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;
SET hive.auto.convert.join=true;

CREATE TABLE test_1 (member BIGINT, attr BIGINT) STORED AS ORC; 

CREATE TABLE test_2 (member BIGINT) STORED AS ORC;

INSERT INTO test_1 VALUES (3,1),(2,2); 
INSERT INTO test_2 VALUES (1),(2),(3),(4); 

-- Add a single NULL row that will come from ORC as isRepeated.
insert into test_1 values (NULL, NULL);
insert into test_2 values (NULL);


EXPLAIN VECTORIZATION DETAIL
SELECT m.member, (CASE WHEN COALESCE(n.attr, 5)>1 THEN n.attr END) AS attr 
FROM test_2 m LEFT JOIN test_1 n ON m.member = n.member; 

SELECT m.member, (CASE WHEN COALESCE(n.attr, 5)>1 THEN n.attr END) AS attr 
FROM test_2 m LEFT JOIN test_1 n ON m.member = n.member; 
