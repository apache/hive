-- HIVE-29580: by-name references to duplicate-named columns that escaped a subquery/CTE
-- boundary; these used to compile under CBO with the reference silently bound to an arbitrary
-- candidate. hive.cli.errors.ignore keeps every rejection in one file; the FAILED messages pair
-- to statements by order. Mechanism-specific cases live in the clientnegative ambiguous_col_*.q
-- tests; the non-CBO baseline is ambiguous_col_noncbo_baseline.q.
set hive.cli.errors.ignore=true;

-- the JIRA shape: duplicate alias escapes a CTE, then a qualified reference through the
-- consuming CTE's alias
with bse as (select 'a' as c, 'b' as c),
     tpm as (select * from bse)
select tpm.c from tpm;

-- duplicate alias escapes a UNION ALL branch into the derived-table alias
select t.c from (select 'a' as c, 'b' as c union all select 'x', 'y') t;
