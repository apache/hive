-- SORT_QUERY_RESULTS
-- HIVE-29580: UNION DISTINCT is rewritten into SELECT DISTINCT * over an internal alias, and
-- the rewrite synthesizes by-name group-by references from the RowResolver it enumerated.
-- Duplicate output aliases in the branches must not trip the ambiguity check there: the
-- references are unique by construction. Both statements must compile, deduplicate across all
-- columns, and keep both duplicate columns' values intact.

select 'a' as c, 'b' as c union select 'a', 'b' union select 'a', 'x';

select distinct * from (select 'a' as c, 'b' as c union all select 'a', 'b' union all select 'a', 'x') t;
