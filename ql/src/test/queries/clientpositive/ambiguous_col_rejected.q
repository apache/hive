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

-- the DISTINCT * rewrite must not launder the ambiguity away
select x.c from (select distinct * from (select 'a' as c, 'b' as c) t) x;

-- same laundering point via the UNION DISTINCT rewrite
select z.c from (select 'a' as c, 'b' as c union select 'x' as c, 'y' as c) z;

-- explicit column list renames only the first column; the second column's own alias collides
-- with the assigned name, so the reference is ambiguous again
with renamed(a) as (select 'x' as c, 'y' as a) select renamed.a from renamed;
