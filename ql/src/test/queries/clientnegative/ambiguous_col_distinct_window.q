-- DISTINCT combined with a windowing function is planned as a GROUP BY over all select columns,
-- whose output RowResolver is rebuilt from fresh ColumnInfo copies; the ambiguity marker must
-- survive that rebuild (and RowResolver.add's copy-constructor, which this shape also exercises).
select x.c from (select distinct *, rank() over (order by d) r from (select 'a' as c, 'b' as c, 'x' as d) t) x;
