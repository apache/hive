--! qt:dataset:src
-- The histograms created by ANALYZE TABLE are not deterministic, so mask them:
--! qt:replace:/(Q[0-9]:) *[0-9.]+/$1 #Masked#/
-- Same for the row estimations influenced by the histograms.
-- They are around 5 rows. If they are >= 20 something is really off:
--! qt:replace:/Statistics: Num rows: [0-1]?[0-9] Data size: [0-9]+/Statistics: Num rows: #Masked# Data size: #Masked#/

set hive.fetch.task.conversion=none;
set metastore.stats.fetch.bitvector=true;
set metastore.stats.fetch.kll=true;
set hive.stats.fetch.column.stats=true;

CREATE TABLE sh2a AS (SELECT cast(key as int) as k1, cast(key as int) as k2 FROM src);

DESCRIBE FORMATTED sh2a k1;
DESCRIBE FORMATTED sh2a k2;

explain SELECT 1 FROM sh2a WHERE k1 < 10 AND k2 < 250;
SELECT count(1) FROM sh2a WHERE k1 < 10 AND k2 < 250;

set hive.stats.kll.enable=true;
CREATE TABLE sh2b AS (SELECT cast(key as int) as k1, cast(key as int) as k2 FROM src);
ANALYZE TABLE sh2b COMPUTE STATISTICS FOR COLUMNS;

DESCRIBE FORMATTED sh2b k1;
DESCRIBE FORMATTED sh2b k2;

explain SELECT 1 FROM sh2b WHERE k1 < 10 AND k2 < 250;
