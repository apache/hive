set hive.orc.splits.include.file.footer=true;
set hive.fetch.task.conversion=none;

CREATE TABLE tbl (id INT, name STRING) STORED AS ORC;
INSERT INTO tbl VALUES (1, 'abc');
SELECT * FROM tbl;
