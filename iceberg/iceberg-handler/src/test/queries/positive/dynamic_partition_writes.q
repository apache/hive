drop table if exists tbl_src;
drop table if exists tbl_target_identity;
drop table if exists tbl_target_bucket;


create external table tbl_src (a int, b string) stored by iceberg stored as orc;
insert into tbl_src values (1, 'EUR'), (2, 'EUR'), (3, 'USD'), (4, 'EUR'), (5, 'HUF'), (6, 'USD'), (7, 'USD'), (8, 'PLN'), (9, 'PLN'), (10, 'CZK');
--need at least 2 files to ensure ClusteredWriter encounters out-of-order records
insert into tbl_src values (10, 'EUR'), (20, 'EUR'), (30, 'USD'), (40, 'EUR'), (50, 'HUF'), (60, 'USD'), (70, 'USD'), (80, 'PLN'), (90, 'PLN'), (100, 'CZK');

create external table tbl_target_identity (a int) partitioned by (ccy string) stored by iceberg stored as orc;
explain insert overwrite table tbl_target_identity select * from tbl_src;
insert overwrite table tbl_target_identity select * from tbl_src;
select * from tbl_target_identity order by a;

--bucketed case - although SortedDynPartitionOptimizer kicks in for this case too, its work is futile as it sorts values rather than the computed buckets
--thus we need this case to check that ClusteredWriter allows out-of-order records for bucket partition spec (only)
create external table tbl_target_bucket (a int, ccy string) partitioned by spec (bucket (2, ccy)) stored by iceberg stored as orc;
explain insert into table tbl_target_bucket select * from tbl_src;
insert into table tbl_target_bucket select * from tbl_src;
select * from tbl_target_bucket order by a;