
create table target(a int, b int, c int) stored by iceberg tblproperties('format-version'='2', 'write.merge.mode'='copy-on-write');
create table source(a int, b int, c int) stored by iceberg tblproperties('format-version'='2', 'write.merge.mode'='copy-on-write');

-- empty plan as joining column cannot be null for matched clause
explain cbo
merge into target as t using source as s on t.a = s.a and t.b = s.b
when matched and t.a is null then delete;

explain cbo
merge into target as t using source as s on t.a = s.a and t.b = s.b
when matched and t.a is null then update set b = t.b + 10;

-- non empty plans as condition for all branches are not false
-- matched and <joining col> is null is always false
-- not matched and <joining col> is [not] null is not necessarily always false
explain cbo
merge into target as t using source as s on t.a = s.a and t.b = s.b
when matched and t.a is null then delete
when matched and t.a > 10 then update set b = t.b + 100
when not matched and s.a > 20 then insert values (s.a, s.b, s.c);

explain cbo
merge into target as t using source as s on t.a = s.a and t.b = s.b
when matched and t.a > 10 then delete
when matched and t.a is null then update set b = t.b + 100
when not matched and s.a > 20 then insert values (s.a, s.b, s.c);

explain cbo
merge into target as t using source as s on t.a = s.a and t.b = s.b
when matched and t.a > 10 then delete
when matched and t.a > 20 then update set b = t.b + 100
when not matched and s.a is null then insert values (s.a, s.b, s.c);

explain cbo
merge into target as t using source as s on t.a = s.a and t.b = s.b
when matched and t.a > 10 then delete
when matched and t.a > 20 then update set b = t.b + 100
when not matched and s.a is not null then insert values (s.a, s.b, s.c);
