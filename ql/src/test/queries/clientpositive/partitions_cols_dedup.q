set hive.support.concurrency=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.managed.tables=true;
set hive.create.as.acid=true;
set hive.create.as.insert.only=true;
set hive.default.fileformat.managed=ORC;
set metastore.metadata.transformer.class=org.apache.hadoop.hive.metastore.MetastoreDefaultTransformer;
set hive.metastore.client.capabilities=HIVEFULLACIDREAD,HIVEFULLACIDWRITE,HIVECACHEINVALIDATE,HIVEMANAGESTATS,HIVEMANAGEDINSERTWRITE,HIVEMANAGEDINSERTREAD;
set metastore.client.skip.columns.for.partitions=true;

drop database if exists partition_cols_dedup cascade;
create database partition_cols_dedup;
use partition_cols_dedup;

create transactional table partitioned_table_1 (i1 int, i2 int, i3 int) partitioned by (year int, month int, day int);
insert into partitioned_table_1 values (1,1,1,1111,111,11),(2,2,2,2222,222,22),(3,3,3,3333,333,33),(4,4,4,4444,444,44),(5,5,5,5555,555,55);
select * from partitioned_table_1;

create table ctas_table_1 as select * from partitioned_table_1;
select * from ctas_table_1;

create table insert_overwrite_table (c1 int, c2 int) partitioned by (p1 int, p2 int);
insert overwrite table insert_overwrite_table select i1, i2, year, month from partitioned_table_1;
select * from insert_overwrite_table;