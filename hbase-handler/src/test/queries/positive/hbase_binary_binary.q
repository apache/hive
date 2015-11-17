drop table if exists testhbaseb;
CREATE TABLE testhbaseb (key int, val binary)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
"hbase.columns.mapping" = ":key,cf:val#b"
);
insert into table testhbaseb values(1, 'hello');
insert into table testhbaseb values(2, 'hi');
select * from testhbaseb;
drop table testhbaseb;


