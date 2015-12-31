set hive.optimize.index.filter = true;
set hive.auto.convert.join=false;

CREATE TABLE tbl1(id INT) STORED AS PARQUET;
INSERT INTO tbl1 VALUES(1), (2);

CREATE TABLE tbl2(id INT, value STRING) STORED AS PARQUET;
INSERT INTO tbl2 VALUES(1, 'value1');
INSERT INTO tbl2 VALUES(1, 'value2');

select tbl1.id, t1.value, t2.value
FROM tbl1
JOIN (SELECT * FROM tbl2 WHERE value='value1') t1 ON tbl1.id=t1.id
JOIN (SELECT * FROM tbl2 WHERE value='value2') t2 ON tbl1.id=t2.id;
