-- SORT_QUERY_RESULTS

CREATE TABLE T1_n1x(key STRING, val STRING) STORED AS orc;
CREATE TABLE T2_n1x(key STRING, val STRING) STORED AS orc;

insert into T1_n1x values('aaa', '111'),('bbb', '222'),('ccc', '333');
insert into T2_n1x values('aaa', '111'),('ddd', '444'),('ccc', '333');

EXPLAIN
SELECT a.key, b.key FROM UNIQUEJOIN PRESERVE T1_n1x a (a.key), PRESERVE  T2_n1x b (b.key);
SELECT a.key, b.key FROM UNIQUEJOIN PRESERVE T1_n1x a (a.key), PRESERVE  T2_n1x b (b.key);

EXPLAIN
SELECT a.key, b.key FROM UNIQUEJOIN T1_n1x a (a.key), T2_n1x b (b.key);
SELECT a.key, b.key FROM UNIQUEJOIN T1_n1x a (a.key), T2_n1x b (b.key);
