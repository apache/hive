create table ice_tbl (id int, name string) Stored by Iceberg;

alter table ice_tbl create branch test_branch_1;
