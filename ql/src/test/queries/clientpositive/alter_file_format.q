create table alter_file_format_test (key int, value string);
desc FORMATTED alter_file_format_test;

alter table alter_file_format_test set fileformat rcfile;
desc FORMATTED alter_file_format_test;

alter table alter_file_format_test set fileformat textfile;
desc FORMATTED alter_file_format_test;

alter table alter_file_format_test set fileformat rcfile;
desc FORMATTED alter_file_format_test;

alter table alter_file_format_test set fileformat sequencefile;
desc FORMATTED alter_file_format_test;

alter table alter_file_format_test set fileformat parquet;
desc FORMATTED alter_file_format_test;

ALTER TABLE alter_file_format_test SET FILEFORMAT INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat' SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe';
desc FORMATTED alter_file_format_test;

drop table alter_partition_format_test_n0;

--partitioned table
create table alter_partition_format_test_n0 (key int, value string) partitioned by (ds string);

alter table alter_partition_format_test_n0 add partition(ds='2010');
desc FORMATTED alter_partition_format_test_n0 partition(ds='2010');

alter table alter_partition_format_test_n0 partition(ds='2010') set fileformat rcfile;
desc FORMATTED alter_partition_format_test_n0 partition(ds='2010');

alter table alter_partition_format_test_n0 partition(ds='2010') set fileformat textfile;
desc FORMATTED alter_partition_format_test_n0 partition(ds='2010');

alter table alter_partition_format_test_n0 partition(ds='2010') set fileformat rcfile;
desc FORMATTED alter_partition_format_test_n0 partition(ds='2010');

alter table alter_partition_format_test_n0 partition(ds='2010') set fileformat sequencefile;
desc FORMATTED alter_partition_format_test_n0 partition(ds='2010');

alter table alter_partition_format_test_n0 partition(ds='2010') set fileformat parquet;
desc FORMATTED alter_partition_format_test_n0 partition(ds='2010');

drop table alter_partition_format_test_n0;