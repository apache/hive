-- HIVE-29580: queries whose by-name reference targets a duplicate-named column that escaped a
-- subquery/CTE boundary. Before HIVE-29580 these compiled under CBO with the reference silently
-- bound to an arbitrary candidate; they are rejected now. hive.cli.errors.ignore lets all
-- rejections live in one reviewable file (resourceplan.q precedent); each statement's FAILED
-- message appears in the golden in statement order. Mechanism-specific rejections have their own
-- clientnegative tests: ambiguous_col{,_2,_ctas,_distinct_window,_join_cond,_join_cond_unqual,
-- _join_using,_lateral_view_alias,_unqualified_ref}.q. The non-CBO baseline for these shapes is
-- ambiguous_col_noncbo_baseline.q.
set hive.cli.errors.ignore=true;

-- the JIRA shape: duplicate alias escapes a CTE, then a qualified reference through the
-- consuming CTE's alias
with bse as (select 'a' as c, 'b' as c),
     tpm as (select * from bse)
select tpm.c from tpm;

-- duplicate alias escapes a UNION ALL branch into the derived-table alias
select t.c from (select 'a' as c, 'b' as c union all select 'x', 'y') t;
