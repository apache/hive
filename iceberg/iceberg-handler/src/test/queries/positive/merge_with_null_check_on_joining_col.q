
create table target(a int, b int, c int) stored by iceberg tblproperties('format-version'='2', 'write.merge.mode'='copy-on-write');
create table source(a int, b int, c int) stored by iceberg tblproperties('format-version'='2', 'write.merge.mode'='copy-on-write');

-- empty plan as joining column cannot be null for matched clause
explain cbo
merge into target as t using source as s on t.a = s.a and t.b = s.b
when matched and t.a is null then delete;

explain cbo
merge into target as t using source as s on t.a = s.a and t.b = s.b
when matched and t.a is null then update set b = t.b + 10;
--------------------------------------------------------------------

 -- non empty plans for these queries
explain cbo
merge into target as t using source as s on t.a = s.a and t.b = s.b
when not matched and t.a is null then insert values (1, 2, 3);

explain cbo
merge into target as t using source as s on t.a = s.a and t.b = s.b
when matched and t.a is null then delete
when matched then update set b = t.b + 10
when not matched then insert values (1, 2, 3);
