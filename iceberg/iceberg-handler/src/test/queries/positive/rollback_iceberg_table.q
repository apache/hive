-- Mask the totalSize value as it can have slight variability, causing test flakiness
--! qt:replace:/(\s+totalSize\s+)\S+(\s+)/$1#Masked#$2/
-- Mask random uuid
--! qt:replace:/(\s+uuid\s+)\S+(\s*)/$1#Masked#$2/

drop table if exists tbl_ice;
create external table tbl_ice(a int, b string, c int) stored by iceberg stored as parquet;
explain alter table tbl_ice execute rollback(11111);
explain alter table tbl_ice execute rollback('2022-05-12 00:00:00');