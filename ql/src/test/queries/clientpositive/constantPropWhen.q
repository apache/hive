set hive.mapred.mode=nonstrict;
set hive.optimize.constant.propagation=false;

drop table test_1_n4; 

create table test_1_n4 (id int, id2 int); 

insert into table test_1_n4 values (123, NULL), (NULL, NULL), (NULL, 123), (123, 123);

explain SELECT cast(CASE WHEN id = id2 THEN FALSE ELSE TRUE END AS BOOLEAN) AS b FROM test_1_n4; 

SELECT cast(CASE WHEN id = id2 THEN FALSE ELSE TRUE END AS BOOLEAN) AS b FROM test_1_n4; 

explain SELECT cast(CASE id when id2 THEN FALSE ELSE TRUE END AS BOOLEAN) AS b FROM test_1_n4;

SELECT cast(CASE id when id2 THEN FALSE ELSE TRUE END AS BOOLEAN) AS b FROM test_1_n4;

explain SELECT cast(CASE WHEN id = id2 THEN TRUE ELSE FALSE END AS BOOLEAN) AS b FROM test_1_n4; 

SELECT cast(CASE WHEN id = id2 THEN TRUE ELSE FALSE END AS BOOLEAN) AS b FROM test_1_n4; 

explain SELECT cast(CASE id when id2 THEN TRUE ELSE FALSE END AS BOOLEAN) AS b FROM test_1_n4;

SELECT cast(CASE id when id2 THEN TRUE ELSE FALSE END AS BOOLEAN) AS b FROM test_1_n4;


set hive.cbo.enable=false;
set hive.optimize.constant.propagation=true;

explain SELECT cast(CASE WHEN id = id2 THEN FALSE ELSE TRUE END AS BOOLEAN) AS b FROM test_1_n4; 

SELECT cast(CASE WHEN id = id2 THEN FALSE ELSE TRUE END AS BOOLEAN) AS b FROM test_1_n4; 

explain SELECT cast(CASE id when id2 THEN FALSE ELSE TRUE END AS BOOLEAN) AS b FROM test_1_n4;

SELECT cast(CASE id when id2 THEN FALSE ELSE TRUE END AS BOOLEAN) AS b FROM test_1_n4;

explain SELECT cast(CASE WHEN id = id2 THEN TRUE ELSE FALSE END AS BOOLEAN) AS b FROM test_1_n4; 

SELECT cast(CASE WHEN id = id2 THEN TRUE ELSE FALSE END AS BOOLEAN) AS b FROM test_1_n4; 

explain SELECT cast(CASE id when id2 THEN TRUE ELSE FALSE END AS BOOLEAN) AS b FROM test_1_n4;

SELECT cast(CASE id when id2 THEN TRUE ELSE FALSE END AS BOOLEAN) AS b FROM test_1_n4;

