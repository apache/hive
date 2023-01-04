create table t2(a integer, b string) STORED AS ORC;
insert into t2 values (1, 'A1'),(2, 'A2'),(3, 'A3'),(4, 'A4'),(5, 'A5'),
                      (6, 'B1'),(7, 'B2'),(8, 'B3'),(9, 'B4'),(20, 'B5');
analyze table t2 compute statistics for columns;

set hive.stats.fetch.column.stats=true;

set hive.stats.use.bitvectors=false;

-- 1,2,10,11,12,13,14,15,20 => 9
explain select a from t2 where a IN (-1,0,1,2,10,11,12,13,14,15,20,30,40) order by a;

set hive.stats.use.bitvectors=true;

-- 1,2,20 => 3
explain select a from t2 where a IN (-1,0,1,2,10,11,12,13,14,15,20,30,40) order by a;

-- A3 only => 1
explain select a from t2 where b IN ('A3', 'ABC', 'AXZ') order by a;

-- A3,B1,B5 => 3
explain select a from t2 where b IN ('A3', 'B1', 'B5') order by a;