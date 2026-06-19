set hive.vectorized.execution.enabled=false;
set hive.optimize.index.filter = true;
set hive.auto.convert.join=false;

CREATE TABLE tbl1_n6(id INT) STORED AS PARQUET;
INSERT INTO tbl1_n6 VALUES(1), (2);

CREATE TABLE tbl2_n5(id INT, value STRING) STORED AS PARQUET;
INSERT INTO tbl2_n5 VALUES(1, 'value1');
INSERT INTO tbl2_n5 VALUES(1, 'value2');

select tbl1_n6.id, t1.value, t2.value
FROM tbl1_n6
JOIN (SELECT * FROM tbl2_n5 WHERE value='value1') t1 ON tbl1_n6.id=t1.id
JOIN (SELECT * FROM tbl2_n5 WHERE value='value2') t2 ON tbl1_n6.id=t2.id;
