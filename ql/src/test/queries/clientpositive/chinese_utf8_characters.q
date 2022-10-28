CREATE EXTERNAL TABLE tbl_chinese_chars(a int, b string, c string);
INSERT INTO tbl_chinese_chars values(1,'上海','徐汇'),(2,'北京','海淀');

CREATE EXTERNAL TABLE tbl_chinese_chars_multidelimitserde (col1 varchar(100), col2 varchar(100))
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ('field.delim'='|~|', 'serialization.encoding'='UTF-8')
STORED AS TEXTFILE;
INSERT INTO TABLE tbl_chinese_chars_multidelimitserde values('测试1','测试2');


set hive.fetch.task.conversion=more;
EXPLAIN SELECT * FROM default.tbl_chinese_chars where b='北京';
SELECT * FROM default.tbl_chinese_chars where b='北京';

set hive.fetch.task.conversion=none;
EXPLAIN SELECT * FROM default.tbl_chinese_chars where b='北京';
SELECT * FROM default.tbl_chinese_chars where b='北京';


set hive.fetch.task.conversion=more;
SELECT * FROM default.tbl_chinese_chars_multidelimitserde;
EXPLAIN SELECT * FROM default.tbl_chinese_chars_multidelimitserde where col1 = '测试1';
SELECT * FROM default.tbl_chinese_chars_multidelimitserde where col1 = '测试1';


set hive.fetch.task.conversion=none;
SELECT * FROM default.tbl_chinese_chars_multidelimitserde;
EXPLAIN SELECT * FROM default.tbl_chinese_chars_multidelimitserde where col1 = '测试1';
SELECT * FROM default.tbl_chinese_chars_multidelimitserde where col1 = '测试1';
