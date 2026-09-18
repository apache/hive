-- the duplicate alias is referenced only in the join condition, via a qualified name.
-- Unparse translation is disabled below on purpose: when it is on, the ON clause is also
-- walked by the generic type check, which would make this test pass even if the ambiguity
-- check in JoinCondTypeCheckProcFactory were removed.
set hive.materializedview.rewriting.sql=false;
set hive.materializedview.rewriting.sql.subquery=false;
select t.d from (select 'a' as c, 'b' as c, 'x' as d) t join (select 'a' as e) u on t.c = u.e;
