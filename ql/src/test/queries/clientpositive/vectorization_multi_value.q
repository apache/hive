
DROP TABLE IF EXISTS cond_vector;
CREATE TABLE cond_vector(a STRING) STORED AS ORC;
INSERT OVERWRITE TABLE cond_vector VALUES("a/b"),("a/b"),("c/d");
set hive.fetch.task.conversion=minimal;
set hive.execution.mode=container;

SELECT IF(1=1, MAP("a", "b"), NULL) FROM cond_vector;
EXPLAIN VECTORIZATION DETAIL SELECT IF(1=1, MAP("a", "b"), NULL) FROM cond_vector;

SELECT IF(1=1, MAP("a", MAP("b","c")), NULL) FROM cond_vector;
EXPLAIN VECTORIZATION DETAIL SELECT IF(1=1, MAP("a", MAP("b","c")), NULL) FROM cond_vector;

SELECT IF(1=1, MAP("a", a), NULL) FROM cond_vector;
EXPLAIN VECTORIZATION DETAIL SELECT IF(1=1, MAP("a", a), NULL) FROM cond_vector;

SELECT IF(1=1, MAP("a", MAP("b", a)), NULL) FROM cond_vector;
EXPLAIN VECTORIZATION DETAIL SELECT IF(1=1, MAP("a", MAP("b", a)), NULL) FROM cond_vector;

SELECT IF(1=1, ARRAY("a", "b"), NULL) FROM cond_vector;
EXPLAIN VECTORIZATION DETAIL SELECT IF(1=1, ARRAY("a", "b"), NULL) FROM cond_vector;

SELECT IF(1=1, ARRAY(ARRAY("a", "b"), ARRAY("c", "d")), NULL) FROM cond_vector;
EXPLAIN VECTORIZATION DETAIL SELECT IF(1=1, ARRAY(ARRAY("a", "b"), ARRAY("c", "d")), NULL) FROM cond_vector;

SELECT IF(1=1, ARRAY("a", a), NULL) FROM cond_vector;
EXPLAIN VECTORIZATION DETAIL SELECT IF(1=1, ARRAY("a", a), NULL) FROM cond_vector;

SELECT IF(1=1, ARRAY(ARRAY("a", a), ARRAY("b", "c")), NULL) FROM cond_vector;
EXPLAIN VECTORIZATION DETAIL SELECT IF(1=1, ARRAY(ARRAY("a", a), ARRAY("b", "c")), NULL) FROM cond_vector;
