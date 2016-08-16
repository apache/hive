set hive.mapred.mode=nonstrict;
set hive.compute.query.using.stats=false;

DROP TABLE IF EXISTS symlink_text_input_format;

EXPLAIN
CREATE TABLE symlink_text_input_format (key STRING, value STRING) STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat';

CREATE TABLE symlink_text_input_format (key STRING, value STRING) STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat';

dfs -cp ../../data/files/symlink1.txt ${system:test.warehouse.dir}/symlink_text_input_format/symlink1.txt;
dfs -cp ../../data/files/symlink2.txt ${system:test.warehouse.dir}/symlink_text_input_format/symlink2.txt;

EXPLAIN SELECT * FROM symlink_text_input_format order by key, value;

SELECT * FROM symlink_text_input_format order by key, value;

EXPLAIN SELECT value FROM symlink_text_input_format order by value;

SELECT value FROM symlink_text_input_format order by value;

EXPLAIN SELECT count(1) FROM symlink_text_input_format;

SELECT count(1) FROM symlink_text_input_format;

DROP TABLE symlink_text_input_format;

CREATE TABLE symlink_text_input_format (key STRING, value STRING) STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat';

dfs -cp ../../data/files/symlink-with-regex.txt ${system:test.warehouse.dir}/symlink_text_input_format/symlink-with-regex.txt;
dfs -cp ../../data/files/symlink2.txt ${system:test.warehouse.dir}/symlink_text_input_format/symlink2.txt;

EXPLAIN SELECT * FROM symlink_text_input_format order by key, value;

SELECT * FROM symlink_text_input_format order by key, value;

EXPLAIN SELECT value FROM symlink_text_input_format order by value;

SELECT value FROM symlink_text_input_format order by value;

EXPLAIN SELECT count(1) FROM symlink_text_input_format;

SELECT count(1) FROM symlink_text_input_format;

SET hive.rework.mapredwork = true ;
SET mapred.max.split.size= 0 ;
SET mapred.min.split.size.per.node= 0 ;
SET mapred.min.split.size.per.rack= 0 ;

SELECT count(1) FROM symlink_text_input_format;

DROP TABLE symlink_text_input_format;
