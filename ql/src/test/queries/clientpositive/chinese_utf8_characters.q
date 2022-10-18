CREATE EXTERNAL TABLE tbl_chinese_chars(a int, b string, c string);
INSERT INTO tbl_chinese_chars values(1,'上海','徐汇'),(2,'北京','海淀');

set hive.fetch.task.conversion=more;
EXPLAIN SELECT * FROM default.tbl_chinese_chars where b='北京';
SELECT * FROM default.tbl_chinese_chars where b='北京';

set hive.fetch.task.conversion=none;
EXPLAIN SELECT * FROM default.tbl_chinese_chars where b='北京';
SELECT * FROM default.tbl_chinese_chars where b='北京';
