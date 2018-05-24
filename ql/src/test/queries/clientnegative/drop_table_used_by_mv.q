create table mytable (key int, value string);
insert into mytable values (1, 'val1'), (2, 'val2');

create materialized view mv1 disable rewrite as
select key, value from mytable;

drop table mytable;

