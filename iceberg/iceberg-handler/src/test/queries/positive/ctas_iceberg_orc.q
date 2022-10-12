create table source(a int, b string, c int);

insert into source values (1, 'one', 3);
insert into source values (1, 'two', 4);

explain
create external table tbl_ice stored by iceberg stored as orc tblproperties ('format-version'='2') as
select a, b, c from source;

create external table tbl_ice stored by iceberg stored as orc tblproperties ('format-version'='2') as
select a, b, c from source;

select * from tbl_ice;
