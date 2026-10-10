-- same as ambiguous_col_join_cond.q but the reference is unqualified, which is resolved by a
-- different processor override. See that file for why unparse translation is disabled here.
set hive.materializedview.rewriting.sql=false;
set hive.materializedview.rewriting.sql.subquery=false;
select t.d from (select 'a' as c, 'b' as c, 'x' as d) t join (select 'a' as e) u on c = u.e;
