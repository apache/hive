--! set hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.LineageLogger;
create table tbl1(col1 string, col2 string);
create table tbl2 as (select * from tbl1);
create table tbl3 partitioned by (col2) as (select * from tbl1);
