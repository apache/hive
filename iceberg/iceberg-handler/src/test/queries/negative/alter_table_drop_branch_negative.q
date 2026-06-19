create table iceTbl (id int, name string) Stored by Iceberg;
insert into iceTbl values(1, 'jack');

-- drop a non-exist branch
alter table iceTbl drop branch test_branch;