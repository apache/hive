-- this test case is to check the behavior of mandarin characters with different serdes and scenarios
-- while it still passes without the corresponding fixes (HIVE-28544), the behavior is not correct
-- if the qtest is run when mimicking a different default charset, like: US-ASCII,
-- which can be achieved by adding this to the command line while running the qtest:
-- -Dmaven.test.jvm.args="-Dfile.encoding=US-ASCII"

CREATE EXTERNAL TABLE tbl_chinese_chars(a int, b varchar(100), c char(100), d string);
INSERT INTO tbl_chinese_chars values(1,'上海1_1','徐汇1_2', '徐上1_3'),(2,'北京2_1','海淀2_2', '徐上2_3');

CREATE EXTERNAL TABLE tbl_chinese_chars_multidelimitserde (a int, b varchar(100), c char(100), d string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ('field.delim'='|~|', 'serialization.encoding'='UTF-8')
STORED AS TEXTFILE;
INSERT INTO TABLE tbl_chinese_chars_multidelimitserde  values(1,'上海1_1','徐汇1_2', '徐上1_3'),(2,'北京2_1','海淀2_2', '徐上2_3');

CREATE EXTERNAL TABLE tbl_chinese_chars_orc  (a int, b varchar(100), c char(100), d string)
STORED AS ORC;
INSERT INTO TABLE tbl_chinese_chars_orc  values(1,'上海1_1','徐汇1_2', '徐上1_3'),(2,'北京2_1','海淀2_2', '徐上2_3');


set hive.fetch.task.conversion=more;
EXPLAIN SELECT * FROM default.tbl_chinese_chars where b='上海1_1';
SELECT * FROM default.tbl_chinese_chars where b='上海1_1';

set hive.fetch.task.conversion=none;
EXPLAIN SELECT * FROM default.tbl_chinese_chars where b='上海1_1';
SELECT * FROM default.tbl_chinese_chars where b='上海1_1';


set hive.fetch.task.conversion=more;
SELECT * FROM default.tbl_chinese_chars_multidelimitserde;
EXPLAIN SELECT * FROM default.tbl_chinese_chars_multidelimitserde where b = '上海1_1';
SELECT * FROM default.tbl_chinese_chars_multidelimitserde where b = '上海1_1';


set hive.fetch.task.conversion=none;
SELECT * FROM default.tbl_chinese_chars_multidelimitserde;
EXPLAIN SELECT * FROM default.tbl_chinese_chars_multidelimitserde where b = '上海1_1';
SELECT * FROM default.tbl_chinese_chars_multidelimitserde where b = '上海1_1';


set hive.fetch.task.conversion=more;
SELECT * FROM default.tbl_chinese_chars_orc;
EXPLAIN SELECT * FROM default.tbl_chinese_chars_orc where b = '上海1_1';
SELECT * FROM default.tbl_chinese_chars_orc where b = '上海1_1';

set hive.fetch.task.conversion=none;
SELECT * FROM default.tbl_chinese_chars_orc;
EXPLAIN SELECT * FROM default.tbl_chinese_chars_orc where b = '上海1_1';
SELECT * FROM default.tbl_chinese_chars_orc where b = '上海1_1';