CREATE TABLE t_struct (id INT) STORED BY ICEBERG;

INSERT INTO t_struct (id) VALUES (1);

ALTER TABLE t_struct ADD COLUMNS (point STRUCT<x:INT, y:INT>);

SELECT * FROM t_struct ORDER BY id;

CREATE TABLE t_map (id INT) STORED BY ICEBERG;

INSERT INTO t_map (id) VALUES (2);

ALTER TABLE t_map ADD COLUMNS (point map<string,string>);

SELECT * FROM t_map ORDER BY id;

CREATE TABLE t_arr (id INT) STORED BY ICEBERG;

INSERT INTO t_arr (id) VALUES (3);

ALTER TABLE t_arr ADD COLUMNS (point array<int>);

SELECT * FROM t_arr ORDER BY id;