create table target(a int, b int) stored by iceberg tblproperties('format-version'='2', 'write.merge.mode'='copy-on-write');
create table source(a int, b int) stored by iceberg tblproperties('format-version'='2', 'write.merge.mode'='copy-on-write');

merge into target as t using source as s on t.a = s.a
when matched and t.a is null then delete;
