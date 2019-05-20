set hive.auto.convert.join=true;

DROP TABLE IF EXISTS test_1; 
CREATE TABLE test_1 
( 
member BIGINT 
, age VARCHAR (100) 
) 
STORED AS TEXTFILE 
;
DROP TABLE IF EXISTS test_2; 
CREATE TABLE test_2 
( 
member BIGINT 
) 
STORED AS TEXTFILE 
;
INSERT INTO test_1 VALUES (1, '20'), (2, '30'), (3, '40'); 
INSERT INTO test_2 VALUES (1), (2), (3);

EXPLAIN
SELECT 
t2.member 
, t1.age_1 
, t1.age_2 
FROM 
test_2 t2 
LEFT JOIN ( 
SELECT 
member 
, age as age_1 
, age as age_2 
FROM 
test_1 
) t1 
ON t2.member = t1.member 
;

SELECT 
t2.member 
, t1.age_1 
, t1.age_2 
FROM 
test_2 t2 
LEFT JOIN ( 
SELECT 
member 
, age as age_1 
, age as age_2 
FROM 
test_1 
) t1 
ON t2.member = t1.member 
;

