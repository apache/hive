create temporary table tmp1 (c1 string);
create index tmp1_idx on table tmp1 (c1) as 'COMPACT' with deferred rebuild;
