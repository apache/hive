-- SORT_QUERY_RESULTS
set hive.explain.user=false;

drop table if exists target_ice;
drop table if exists source;

create external table target_ice(a int, b string, c int) partitioned by spec (bucket(16, a), truncate(3, b)) stored by iceberg tblproperties ('format-version'='2', 'write.merge.mode'='copy-on-write');
create table source(a int, b string, c int);

insert into target_ice values (1, 'one', 50), (2, 'two', 51), (111, 'one', 55), (333, 'two', 56);
insert into source values (1, 'one', 50), (2, 'two', 51), (3, 'three', 52), (4, 'four', 53), (5, 'five', 54), (111, 'one', 55);

-- merge
explain
merge into target_ice as t using source src ON t.a = src.a
when matched and t.a > 100 THEN DELETE
when matched then update set b = 'Merged', c = t.c + 10;

merge into target_ice as t using source src ON t.a = src.a
when matched and t.a > 100 THEN DELETE
when matched then update set b = 'Merged', c = t.c + 10;

select * from target_ice;

explain
merge into target_ice as t using source src ON t.a = src.a
when not matched then insert values (src.a, src.b, src.c);

merge into target_ice as t using source src ON t.a = src.a
when not matched then insert values (src.a, src.b, src.c);

select * from target_ice;
