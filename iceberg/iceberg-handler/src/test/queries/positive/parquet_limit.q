create table icebergtable (id int, name string) stored by iceberg stored as parquet;

insert into icebergtable values (1, 'Joe'), (2, 'Jack');

select * from icebergtable limit 1;
