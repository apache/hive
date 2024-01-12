set hive.mapred.mode=nonstrict;
set hive.explain.user=false;

create temporary table foo (col string) stored as orc;
create temporary table bar (col binary) stored as orc;

-- SORT_QUERY_RESULTS

INSERT INTO bar values(unhex('6161-16161'));
INSERT INTO foo SELECT col FROM bar;

explain select col from foo where col like '%a%';
select col from foo where col like '%a%';

