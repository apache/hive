set hive.stats.kll.enable=true;
set metastore.stats.fetch.bitvector=true;

CREATE TABLE tab1 AS (SELECT 1 as key);

DESCRIBE FORMATTED tab1 key;
set metastore.stats.fetch.kll=true;

CREATE TABLE tab2 AS (SELECT 1 as key);

ANALYZE TABLE tab2 COMPUTE STATISTICS FOR COLUMNS;
DESCRIBE FORMATTED tab2 key;

DROP TABLE tab1;
DROP TABLE tab2;
