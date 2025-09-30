CREATE TABLE t_complex (id INT) STORED BY ICEBERG;

INSERT INTO t_complex (id) VALUES (1);

ALTER TABLE t_complex ADD COLUMNS (col1 STRUCT<x:INT, y:INT>);

INSERT INTO t_complex VALUES (2, named_struct("x", 10, "y", 20));

SELECT * FROM t_complex ORDER BY id;

ALTER TABLE t_complex ADD COLUMNS (col2 map<string,string>);

INSERT INTO t_complex VALUES (3, named_struct("x", 11, "y", 22), map("k1", "v1", "k2", "v2"));

SELECT * FROM t_complex ORDER BY id;

ALTER TABLE t_complex ADD COLUMNS (col3 array<int>);

INSERT INTO t_complex VALUES (4, named_struct("x", 5, "y", 18),  map("k22", "v22", "k33", "v44"), array(1, 2, 3));

SELECT * FROM t_complex ORDER BY id;