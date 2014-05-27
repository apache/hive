create table alter_file_format_test (key int, value string);
desc FORMATTED alter_file_format_test;

ALTER TABLE alter_file_format_test SET FILEFORMAT INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat';

drop table alter_partition_format_test;