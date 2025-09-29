CREATE TABLE t_complex (id INT) STORED BY ICEBERG;

INSERT INTO t_complex (id) VALUES (1);

ALTER TABLE t_complex ADD COLUMNS (col1 STRUCT<x:INT, y:INT>);

SELECT * FROM t_complex ORDER BY id;

ALTER TABLE t_complex ADD COLUMNS (col2 map<string,string>);

SELECT * FROM t_complex ORDER BY id;

ALTER TABLE t_complex ADD COLUMNS (col3 array<int>);

SELECT * FROM t_complex ORDER BY id;