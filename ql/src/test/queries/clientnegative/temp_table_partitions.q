-- temp tables with partition columns not currently supported
create temporary table tmp1 (c1 string) partitioned by (p1 string);
