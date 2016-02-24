SET hive.exec.schema.evolution=false;
create table src_orc (key tinyint, val string) clustered by (val) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');
alter table src_orc set serde 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe';
