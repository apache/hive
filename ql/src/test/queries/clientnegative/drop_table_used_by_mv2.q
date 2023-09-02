create table mytable (key int, value string);
insert into mytable values (1, 'val1'), (2, 'val2');
create view myview as select * from mytable;

create materialized view mv1 disable rewrite as
select key, value from myview;
create materialized view mv2 disable rewrite as
select count(*) from myview;

-- dropping the view is fine, as the MV uses not the view itself, but it's query for creating it's own during it's creation
drop view myview;
drop table mytable;
