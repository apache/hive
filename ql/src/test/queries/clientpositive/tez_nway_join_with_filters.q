set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.auto.convert.join=true;

create temporary table foo (id int, code int) stored as orc;
create temporary table bar (id int, code int) stored as orc;
create temporary table baz (id int) stored as orc;


-- SORT_QUERY_RESULTS

insert into foo values(1, 1),(2, 2),(3, 3),(4, 4);
insert into bar values(2, 2),(4, 4);
insert into baz values(2),(4);

set hive.merge.nway.joins=true;
explain select count(*) from foo a left outer join bar b on a.id = b.id and (a.code = 2 or a.code = 4) left outer join baz c on a.id = c.id;

set hive.merge.nway.joins=false;
explain select count(*) from foo a left outer join bar b on a.id = b.id and (a.code = 2 or a.code = 4) left outer join baz c on a.id = c.id;

set hive.merge.nway.joins=true;
select count(*) from foo a left outer join bar b on a.id = b.id and (a.code = 2 or a.code = 4) left outer join baz c on a.id = c.id;

set hive.merge.nway.joins=false;
select count(*) from foo a left outer join bar b on a.id = b.id and (a.code = 2 or a.code = 4) left outer join baz c on a.id = c.id;

set hive.merge.nway.joins=true;
explain select count(*) from foo a inner join bar b on a.id = b.id and (a.code = 2 or a.code = 4) left outer join baz c on a.id = c.id;

set hive.merge.nway.joins=false;
explain select count(*) from foo a inner join bar b on a.id = b.id and (a.code = 2 or a.code = 4) left outer join baz c on a.id = c.id;

set hive.merge.nway.joins=true;
select count(*) from foo a inner join bar b on a.id = b.id and (a.code = 2 or a.code = 4) left outer join baz c on a.id = c.id;

set hive.merge.nway.joins=false;
select count(*) from foo a inner join bar b on a.id = b.id and (a.code = 2 or a.code = 4) left outer join baz c on a.id = c.id;
