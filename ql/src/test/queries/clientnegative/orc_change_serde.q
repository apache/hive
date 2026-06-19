SET hive.exec.schema.evolution=true;
create table src_orc (key tinyint, val string) stored as orc;
alter table src_orc set serde 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe';
