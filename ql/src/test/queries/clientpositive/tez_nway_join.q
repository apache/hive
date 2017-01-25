set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.auto.convert.join=true;

create temporary table foo (key int) stored as orc;
create temporary table bar (key int) stored as orc;

-- SORT_QUERY_RESULTS

insert into foo values(1),(2),(3);
insert into bar values(2),(4);

set hive.merge.nway.joins=true;
explain select count(*) from foo a join bar b on (a.key = b.key) join bar c on (a.key = c.key);

set hive.merge.nway.joins=false;
explain select count(*) from foo a join bar b on (a.key = b.key) join bar c on (a.key = c.key);

set hive.merge.nway.joins=true;
select count(*) from foo a join bar b on (a.key = b.key) join bar c on (a.key = c.key);

set hive.merge.nway.joins=false;
select count(*) from foo a join bar b on (a.key = b.key) join bar c on (a.key = c.key);

set hive.merge.nway.joins=true;
explain select count(*) from foo a left outer join bar b on (a.key = b.key) left outer join bar c on (a.key = c.key);

set hive.merge.nway.joins=false;
explain select count(*) from foo a left outer join bar b on (a.key = b.key) left outer join bar c on (a.key = c.key);
