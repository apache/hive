-- test table
create table test_table_n1 (id int, query string, name string);
describe extended test_table_n1;

alter table test_table_n1 set serde 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe';
describe extended test_table_n1;

alter table test_table_n1 set serdeproperties ('field.delim' = ',');
describe extended test_table_n1;

drop table test_table_n1;

--- test partitioned table
create table test_table_n1 (id int, query string, name string) partitioned by (dt string);

alter table test_table_n1 add partition (dt = '2011');
describe extended test_table_n1 partition (dt='2011');

alter table test_table_n1 set serde 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe';
describe extended test_table_n1 partition (dt='2011');

alter table test_table_n1 set serdeproperties ('field.delim' = ',');
describe extended test_table_n1 partition (dt='2011');

-- test partitions

alter table test_table_n1 partition(dt='2011') set serde 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe';
describe extended test_table_n1 partition (dt='2011');

alter table test_table_n1 partition(dt='2011') set serdeproperties ('field.delim' = ',');
describe extended test_table_n1 partition (dt='2011');

drop table test_table_n1
