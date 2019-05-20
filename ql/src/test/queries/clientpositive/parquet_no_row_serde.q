set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;
set hive.vectorized.use.vectorized.input.format=false;
set hive.vectorized.use.row.serde.deserialize=true;

drop table tbl_rc;
drop table tbl_parquet;

create table tbl_rc (val decimal(10,0))
row format serde 'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe' stored as rcfile;
create table tbl_parquet (val decimal(10,0)) 
STORED AS PARQUET;

insert into table tbl_rc values(101);
insert into table tbl_parquet values(101);

explain vectorization expression
select val, round(val, -1) from tbl_rc order by val;
explain vectorization expression
select val, round(val, -1) from tbl_parquet order by val;

drop table tbl_rc;
drop table tbl_parquet;