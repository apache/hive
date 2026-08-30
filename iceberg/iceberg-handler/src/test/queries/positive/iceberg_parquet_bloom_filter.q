-- Mask random uuid
--! qt:replace:/(\s+'uuid'=')\S+('\s*)/$1#Masked#$2/

-- Parquet bloom filter write properties on Iceberg tables: verifies the properties are accepted,
-- survive in HMS, and inserts/point lookups work with bloom filters enabled.
-- Bloom filter presence in the data files is asserted by TestHiveIcebergParquetBloomFilter.

drop table if exists tbl_bloom;
create external table tbl_bloom(id bigint, name string) stored by iceberg stored as parquet
tblproperties ('format-version'='2',
               'write.parquet.bloom-filter-enabled.column.id'='true',
               'write.parquet.bloom-filter-fpp.column.id'='0.05');

show create table tbl_bloom;

insert into tbl_bloom values (1, 'one'), (42, 'answer'), (100, 'hundred'), (12345678, 'big');

select name from tbl_bloom where id = 42;
select count(*) from tbl_bloom where id = 43;
select * from tbl_bloom order by id;

-- enable bloom filter on another column, subsequent writes pick it up
alter table tbl_bloom set tblproperties ('write.parquet.bloom-filter-enabled.column.name'='true');
insert into tbl_bloom values (200, 'two hundred');

select id from tbl_bloom where name = 'two hundred';
select count(*) from tbl_bloom;

drop table tbl_bloom;
