set hive.mapred.mode=nonstrict;
drop table pcs_t1; 
drop table pcs_t2; 

create table pcs_t1 (key int, value string) partitioned by (ds string); 
insert overwrite table pcs_t1 partition (ds='2000-04-08') select * from src where key < 20 order by key; 
insert overwrite table pcs_t1 partition (ds='2000-04-09') select * from src where key < 20 order by key;
insert overwrite table pcs_t1 partition (ds='2000-04-10') select * from src where key < 20 order by key; 

analyze table pcs_t1 partition(ds) compute statistics;
analyze table pcs_t1 partition(ds) compute statistics for columns;

set hive.optimize.point.lookup = true;
set hive.optimize.point.lookup.min = 1;

explain extended select key, value, ds from pcs_t1 where (ds='2000-04-08' and key=1) or (ds='2000-04-09' and key=2) order by key, value, ds;
select key, value, ds from pcs_t1 where (ds='2000-04-08' and key=1) or (ds='2000-04-09' and key=2) order by key, value, ds;

set hive.optimize.point.lookup = false;
set hive.optimize.partition.columns.separate=true;
set hive.optimize.ppd=true;

explain extended select ds from pcs_t1 where struct(ds, key) in (struct('2000-04-08',1), struct('2000-04-09',2));
select ds from pcs_t1 where struct(ds, key) in (struct('2000-04-08',1), struct('2000-04-09',2));

explain extended select ds from pcs_t1 where struct(ds, key+2) in (struct('2000-04-08',3), struct('2000-04-09',4));
select ds from pcs_t1 where struct(ds, key+2) in (struct('2000-04-08',3), struct('2000-04-09',4));

set hive.cbo.enable=false;
explain extended select /*+ MAPJOIN(pcs_t1) */ a.ds, b.key from pcs_t1 a join pcs_t1 b  on a.ds=b.ds where struct(a.ds, a.key, b.ds) in (struct('2000-04-08',1, '2000-04-09'), struct('2000-04-09',2, '2000-04-08'));

select /*+ MAPJOIN(pcs_t1) */ a.ds, b.key from pcs_t1 a join pcs_t1 b  on a.ds=b.ds where struct(a.ds, a.key, b.ds) in (struct('2000-04-08',1, '2000-04-09'), struct('2000-04-09',2, '2000-04-08'));

explain extended select ds from pcs_t1 where struct(ds, key+key) in (struct('2000-04-08',1), struct('2000-04-09',2));
select ds from pcs_t1 where struct(ds, key+key) in (struct('2000-04-08',1), struct('2000-04-09',2));

explain select lag(key) over (partition by key) as c1
from pcs_t1 where struct(ds, key) in (struct('2000-04-08',1), struct('2000-04-09',2));
select lag(key) over (partition by key) as c1
from pcs_t1 where struct(ds, key) in (struct('2000-04-08',1), struct('2000-04-09',2));

EXPLAIN EXTENDED
SELECT * FROM (
  SELECT X.* FROM pcs_t1 X WHERE struct(X.ds, X.key) in (struct('2000-04-08',1), struct('2000-04-09',2))
  UNION ALL
  SELECT Y.* FROM pcs_t1 Y WHERE struct(Y.ds, Y.key) in (struct('2000-04-08',1), struct('2000-04-09',2))
) A
WHERE A.ds = '2008-04-08'
SORT BY A.key, A.value, A.ds;

SELECT * FROM (
  SELECT X.* FROM pcs_t1 X WHERE struct(X.ds, X.key) in (struct('2000-04-08',1), struct('2000-04-09',2))
  UNION ALL
  SELECT Y.* FROM pcs_t1 Y WHERE struct(Y.ds, Y.key) in (struct('2000-04-08',1), struct('2000-04-09',2))
) A
WHERE A.ds = '2008-04-08'
SORT BY A.key, A.value, A.ds;

explain extended select ds from pcs_t1 where struct(case when ds='2000-04-08' then 10 else 20 end) in (struct(10),struct(11));
select ds from pcs_t1 where struct(case when ds='2000-04-08' then 10 else 20 end) in (struct(10),struct(11));

explain extended select ds from pcs_t1 where struct(ds, key, rand(100)) in (struct('2000-04-08',1,0.2), struct('2000-04-09',2,0.3));

explain extended select ds from pcs_t1 where struct(ds='2000-04-08' or key = 2, key) in (struct(true,2), struct(false,3));
select ds from pcs_t1 where struct(ds='2000-04-08' or key = 2, key) in (struct(true,2), struct(false,3));

explain extended select ds from pcs_t1 where key = 3 or (struct(ds='2000-04-08' or key = 2, key) in (struct(true,2), struct(false,3)) and key+5 > 0);
select ds from pcs_t1 where key = 3 or (struct(ds='2000-04-08' or key = 2, key) in (struct(true,2), struct(false,3)) and key+5 > 0);
