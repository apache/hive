-- SORT_QUERY_RESULTS
--! qt:replace:/DeleteMetadataSpec(\S*)/#Masked#/
set hive.explain.user=false;

create table ice_date (a int, b date) stored by iceberg stored as orc tblproperties ('format-version'='2');

insert into table ice_date values (1, '2021-01-01');
insert into table ice_date values (2, '2022-02-02'), (3, '2022-03-03');

delete from ice_date where b = '2022-02-02';
delete from ice_date where a = 1 and b = '2021-01-01';

select * from ice_date;

create table ice_date_year (a int, b date) stored by iceberg stored as orc tblproperties ('format-version'='2');
insert into table ice_date_year values (1, '2021-01-01');
insert into table ice_date_year values (2, '2022-02-02'), (3, '2022-03-03');

delete from ice_date_year where year(b) = 2022;

select * from ice_date_year;

-- Metadata delete should not be done here and fallback to normal delete.
create table ice_str_name (first_name string, last_name string) stored by iceberg stored as orc tblproperties ('format-version'='2');
insert into table ice_str_name values ('Alex', 'Clark');
insert into table ice_str_name values ('Bob', 'Bob');

delete from ice_str_name where first_name = last_name;

select * from ice_str_name;

-- Metadata delete should not be done here and fallback to normal delete.
create table ice_int_id (first_id int, last_id int) stored by iceberg stored as orc tblproperties ('format-version'='2');
insert into table ice_int_id values (7, 9);
insert into table ice_int_id values (8, 8);

delete from ice_int_id where first_id = last_id;

select * from ice_int_id;

-- Check if delete on a branch also uses the metadata delete whenever possible.
create table ice_branch_metadata_delete (a int, b string) stored by iceberg stored as orc tblproperties ('format-version'='2');
insert into table ice_branch_metadata_delete values (1, 'ABC');
insert into table ice_branch_metadata_delete values (2, 'DEF');
insert into table ice_branch_metadata_delete values (3, 'GHI');
insert into table ice_branch_metadata_delete values (4, 'JKL');

alter table ice_branch_metadata_delete create branch test01;
delete from default.ice_branch_metadata_delete.branch_test01 where a = 1;

select * from default.ice_branch_metadata_delete.branch_test01;

alter table ice_branch_metadata_delete drop branch test01;

-- Metadata delete must not be applied for multi-table scans which have subquery and fallback to normal delete logic.
create table ice_delete_multiple_table1 (a int, b string) stored by iceberg stored as orc tblproperties ('format-version' = '2');
create table ice_delete_multiple_table2 (a int, b string) stored by iceberg stored as orc tblproperties ('format-version' = '2');
insert into table ice_delete_multiple_table1 values (1, 'ABC'), (2, 'DEF'), (3, 'GHI');
insert into table ice_delete_multiple_table1 values (4, 'GHI'), (5, 'JKL'), (6, 'PQR');
insert into table ice_delete_multiple_table2 values (1, 'ABC'), (2, 'DEF'), (3, 'GHI');
insert into table ice_delete_multiple_table2 values (4, 'GHI'), (5, 'JKL'), (6, 'PQR');

delete from ice_delete_multiple_table2 where ice_delete_multiple_table2.a in (select ice_delete_multiple_table1.a from ice_delete_multiple_table1 where ice_delete_multiple_table1.b = 'GHI');

select * from ice_delete_multiple_table2;

create table test_delete_config (a int, b int) stored by iceberg stored as orc tblproperties ('format-version'='2');
insert into table test_delete_config values (1,2), (3,4);

explain delete from test_delete_config where b < 5;

set hive.optimize.delete.metadata.only=false;
explain delete from test_delete_config where b < 5;

drop table ice_date;
drop table ice_date_year;
drop table ice_str_name;
drop table ice_int_id;
drop table ice_branch_metadata_delete;
drop table ice_delete_multiple_table1;
drop table ice_delete_multiple_table2;
drop table test_delete_config;
