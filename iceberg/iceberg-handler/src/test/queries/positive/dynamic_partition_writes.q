-- Mask the file size values as it can have slight variability, causing test flakiness
--! qt:replace:/("file_size_in_bytes":)\d+/$1#Masked#/
--! qt:replace:/("total-files-size":)\d+/$1#Masked#/
--! qt:replace:/((ORC|PARQUET|AVRO)\s+\d+\s+)\d+/$1#Masked#/

drop table if exists tbl_src;
drop table if exists tbl_target_identity;
drop table if exists tbl_target_bucket;
drop table if exists tbl_target_mixed;
drop table if exists tbl_bucket_date;

create external table tbl_src (a int, b string, c bigint) stored by iceberg stored as orc;
insert into tbl_src values (1, 'EUR', 10), (2, 'EUR', 10), (3, 'USD', 11), (4, 'EUR', 12), (5, 'HUF', 30), (6, 'USD', 10), (7, 'USD', 100), (8, 'PLN', 20), (9, 'PLN', 11), (10, 'CZK', 5), (12, NULL, NULL);
--need at least 2 files to ensure ClusteredWriter encounters out-of-order records
insert into tbl_src values (10, 'EUR', 12), (20, 'EUR', 11), (30, 'USD', 100), (40, 'EUR', 10), (50, 'HUF', 30), (60, 'USD', 12), (70, 'USD', 20), (80, 'PLN', 100), (90, 'PLN', 18), (100, 'CZK', 12), (110, NULL, NULL);

create external table tbl_target_identity (a int) partitioned by (ccy string) stored by iceberg stored as orc;
explain insert overwrite table tbl_target_identity select a, b from tbl_src;
insert overwrite table tbl_target_identity select a, b from tbl_src;
select * from tbl_target_identity order by a, ccy;

--bucketed case - should invoke GenericUDFIcebergBucket to calculate buckets before sorting
create external table tbl_target_bucket (a int, ccy string) partitioned by spec (bucket (2, ccy)) stored by iceberg stored as orc;
explain insert into table tbl_target_bucket select a, b from tbl_src;
insert into table tbl_target_bucket select a, b from tbl_src;
select * from tbl_target_bucket order by a, ccy;

--mixed case - 1 identity + 1 bucket cols
create external table tbl_target_mixed (a int, ccy string, c bigint) partitioned by spec (ccy, bucket (3, c)) stored by iceberg stored as orc;
explain insert into table tbl_target_mixed select * from tbl_src;
insert into table tbl_target_mixed select * from tbl_src;
select * from tbl_target_mixed order by a, ccy;
select `partition` from default.tbl_target_mixed.partitions order by `partition`;
select * from default.tbl_target_mixed.files;

--1 of 2 partition cols is folded with constant - should still sort
explain insert into table tbl_target_mixed select * from tbl_src where b = 'EUR';
insert into table tbl_target_mixed select * from tbl_src where b = 'EUR';

--all partitions cols folded - should not sort as it's not needed
explain insert into table tbl_target_mixed select * from tbl_src where b = 'USD' and c = 100;
insert into table tbl_target_mixed select * from tbl_src where b = 'USD' and c = 100;

select * from tbl_target_mixed order by a, ccy;
select * from default.tbl_target_mixed.files;

--bucket partition transforms with DATE column type
create external table tbl_bucket_date (id string, date_time_date date, year_partition int) 
    partitioned by spec (year_partition, bucket(1, date_time_date))
stored by iceberg stored as parquet 
tblproperties ('parquet.compression'='snappy','format-version'='2');

insert into tbl_bucket_date values (88669, '2018-05-27', 2018), (40568, '2018-02-12', 2018), (40568, '2018-07-03', 2018);
update tbl_bucket_date set date_time_date = '2018-07-02' where date_time_date = '2018-07-03'; 
    
select count(*) from tbl_bucket_date where date_time_date = '2018-07-02';  
