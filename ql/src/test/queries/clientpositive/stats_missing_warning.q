--! qt:dataset:src
set hive.stats.autogather=false;

-- Explictily DROP vs. CREATE IF NOT EXISTS to ensure stats are not carried over
DROP TABLE IF EXISTS missing_stats_t1;
DROP TABLE IF EXISTS missing_stats_t2;
DROP TABLE IF EXISTS missing_stats_t3;
CREATE TABLE missing_stats_t1 (key STRING, value STRING);
CREATE TABLE missing_stats_t2 (key STRING, value STRING);
CREATE TABLE missing_stats_t3 (key STRING, value STRING);

INSERT INTO missing_stats_t1 (key, value)
   SELECT key, value
   FROM src;

INSERT INTO missing_stats_t2 (key, value)
   SELECT key, value
   FROM src;

INSERT INTO missing_stats_t3 (key, value)
   SELECT key, value
   FROM src;
 
-- Default should be FALSE
set hive.cbo.show.warnings=true;

set hive.cbo.enable=true;

-- Should print warning
set hive.cbo.show.warnings=true;

SELECT COUNT(*)
FROM missing_stats_t1 t1
JOIN missing_stats_t2 t2 ON t1.value = t2.key
JOIN missing_stats_t3 t3 ON t2.key = t3.value;

-- Should not print warning
set hive.cbo.show.warnings=false;

SELECT COUNT(*)
FROM missing_stats_t1 t1
JOIN missing_stats_t2 t2 ON t1.value = t2.key
JOIN missing_stats_t3 t3 ON t2.key = t3.value;

ANALYZE TABLE missing_stats_t1 COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE missing_stats_t2 COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE missing_stats_t3 COMPUTE STATISTICS FOR COLUMNS;


-- Warning should be gone
set hive.cbo.show.warnings=true;

SELECT COUNT(*)
FROM missing_stats_t1 t1
JOIN missing_stats_t2 t2 ON t1.value = t2.key
JOIN missing_stats_t3 t3 ON t2.key = t3.value;
