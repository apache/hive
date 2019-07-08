--! qt:dataset:src1
--! qt:dataset:src
set hive.merge.tezfiles=true;
-- SORT_BEFORE_DIFF

EXPLAIN
CREATE TABLE x AS
  SELECT key, 1 as tag FROM src WHERE key = '238'
  UNION ALL
  SELECT key, tag FROM src1
  LATERAL VIEW EXPLODE(array(2)) tf as tag
  WHERE key = '238';

CREATE TABLE x AS
  SELECT key, 1 as tag FROM src WHERE key = '238'
  UNION ALL
  SELECT key, tag FROM src1
  LATERAL VIEW EXPLODE(array(2)) tf as tag
  WHERE key = '238';

SELECT * FROM x;

