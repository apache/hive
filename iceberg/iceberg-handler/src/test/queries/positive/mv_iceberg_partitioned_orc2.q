-- MV data is stored by partitioned iceberg with partition spec
--! qt:replace:/(\s+uuid\s+)\S+(\s*)/$1#Masked#$2/
-- SORT_QUERY_RESULTS

drop materialized view if exists mat1;
drop table if exists tbl_ice;

create table tbl_ice(a int, b string, c int) stored by iceberg stored as orc tblproperties ('format-version'='1');
insert into tbl_ice values (1, 'one', 50), (2, 'two', 51), (3, 'three', 52), (4, 'four', 53), (5, 'five', 54);

create materialized view mat1 partitioned on spec (bucket(16, b), truncate(3, c)) stored by iceberg stored as orc tblproperties ('format-version'='1') as
select tbl_ice.b, tbl_ice.c from tbl_ice where tbl_ice.c > 52;

describe formatted mat1;

select * from mat1;
