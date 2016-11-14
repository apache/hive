set hive.mapred.mode=nonstrict;
-- non agg, non corr

explain
select key, count(*) 
from src 
group by key
having key not in  
  ( select key  from src s1 
    where s1.key > '12'
  )
;

-- non agg, corr
explain
select b.p_mfgr, min(p_retailprice) 
from part b 
group by b.p_mfgr
having b.p_mfgr not in 
  (select p_mfgr 
  from (select p_mfgr, min(p_retailprice) l, max(p_retailprice) r, avg(p_retailprice) a from part group by p_mfgr) a 
  where min(p_retailprice) = l and r - l > 600
  )
;

select b.p_mfgr, min(p_retailprice) 
from part b 
group by b.p_mfgr
having b.p_mfgr not in 
  (select p_mfgr 
  from (select p_mfgr, min(p_retailprice) l, max(p_retailprice) r, avg(p_retailprice) a from part group by p_mfgr) a 
  where min(p_retailprice) = l and r - l > 600
  )
;

-- agg, non corr
explain
select b.p_mfgr, min(p_retailprice) 
from part b 
group by b.p_mfgr
having b.p_mfgr not in 
  (select p_mfgr 
  from part a
  group by p_mfgr
  having max(p_retailprice) - min(p_retailprice) > 600
  )
;

select b.p_mfgr, min(p_retailprice) 
from part b 
group by b.p_mfgr
having b.p_mfgr not in 
  (select p_mfgr 
  from part a
  group by p_mfgr
  having max(p_retailprice) - min(p_retailprice) > 600
  )
;

--nullability tests
CREATE TABLE t1 (c1 INT, c2 CHAR(100));
INSERT INTO t1 VALUES (null,null), (1,''), (2,'abcde'), (100,'abcdefghij');

CREATE TABLE t2 (c1 INT);
INSERT INTO t2 VALUES (null), (2), (100);

explain SELECT c1 FROM t1 group by c1 having c1 NOT IN (SELECT c1 FROM t2);
SELECT c1 FROM t1 group by c1 having c1 NOT IN (SELECT c1 FROM t2);

explain SELECT c1 FROM t1 group by c1 having c1 NOT IN (SELECT c1 FROM t2 where t1.c1=t2.c1);
SELECT c1 FROM t1 group by c1 having c1 NOT IN (SELECT c1 FROM t2 where t1.c1=t2.c1);

DROP TABLE t1;
DROP TABLE t2;
