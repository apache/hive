create temporary table tmp1 (c1 string);
-- table-level stats should work
analyze table tmp1 compute statistics;
-- column stats should fail
analyze table tmp1 compute statistics for columns;
