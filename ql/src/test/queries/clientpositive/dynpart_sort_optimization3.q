SET hive.explain.user=false;
SET hive.exec.dynamic.partition=true;
SET hive.stats.autogather=false;
SET hive.vectorized.execution.enabled=false;
CREATE TABLE src (key string) STORED AS TEXTFILE;

insert into src values ('2000'), ('60'), ('100'), ('5');

create table ctas_part (key int) partitioned by (modkey bigint);

-- vectorized | order by cast | dynamic partition cast | limit
--     n      |       n       |          y             |   y
TRUNCATE TABLE ctas_part;
EXPLAIN
INSERT OVERWRITE TABLE ctas_part PARTITION (modkey)
SELECT key, ceil(key / 100) FROM src ORDER BY key LIMIT 10;

INSERT OVERWRITE TABLE ctas_part PARTITION (modkey)
SELECT key, ceil(key / 100) FROM src ORDER BY key LIMIT 10;

SELECT * FROM ctas_part;

-- vectorized | order by cast | dynamic partition cast | limit
--     n      |       y       |          y             |   y
TRUNCATE TABLE ctas_part;
EXPLAIN
INSERT OVERWRITE TABLE ctas_part PARTITION (modkey)
SELECT key, ceil(key / 100) FROM src ORDER BY cast(key as int) LIMIT 10;

INSERT OVERWRITE TABLE ctas_part PARTITION (modkey)
SELECT key, ceil(key / 100) FROM src ORDER BY cast(key as int) LIMIT 10;

SELECT * FROM ctas_part;

-- vectorized | order by cast | dynamic partition cast | limit
--     n      |       n       |          y             |   n
TRUNCATE TABLE ctas_part;
EXPLAIN
INSERT OVERWRITE TABLE ctas_part PARTITION (modkey)
SELECT key, ceil(key / 100) FROM src ORDER BY key;

INSERT OVERWRITE TABLE ctas_part PARTITION (modkey)
SELECT key, ceil(key / 100) FROM src ORDER BY key;

SELECT * FROM ctas_part;

-- vectorized | order by cast | dynamic partition cast | limit
--     n      |       y       |          y             |   n
TRUNCATE TABLE ctas_part;
-- TODO: Rows within a partition should be ordered by int comparison
--       of `key` column. Here, ordering is by string comparison of
--       `key` column despite `cast(.. as int)`
EXPLAIN
INSERT OVERWRITE TABLE ctas_part PARTITION (modkey)
SELECT key, ceil(key / 100) FROM src ORDER BY cast(key as int);

INSERT OVERWRITE TABLE ctas_part PARTITION (modkey)
SELECT key, ceil(key / 100) FROM src ORDER BY cast(key as int);

SELECT * FROM ctas_part;

-- vectorized | order by cast | dynamic partition cast | limit
--     n      |       n       |          n             |   y
TRUNCATE TABLE ctas_part;
EXPLAIN
INSERT OVERWRITE TABLE ctas_part PARTITION (modkey)
SELECT key, key FROM src ORDER BY key LIMIT 10;

INSERT OVERWRITE TABLE ctas_part PARTITION (modkey)
SELECT key, key FROM src ORDER BY key LIMIT 10;

SELECT * FROM ctas_part;

-- vectorized | order by cast | dynamic partition cast | limit
--     n      |       y       |          n             |   y
TRUNCATE TABLE ctas_part;
EXPLAIN
INSERT OVERWRITE TABLE ctas_part PARTITION (modkey)
SELECT key, key FROM src ORDER BY cast(key as int) LIMIT 10;

INSERT OVERWRITE TABLE ctas_part PARTITION (modkey)
SELECT key, key FROM src ORDER BY cast(key as int) LIMIT 10;

SELECT * FROM ctas_part;

-- vectorized | order by cast | dynamic partition cast | limit
--     n      |       n       |          n             |   n
TRUNCATE TABLE ctas_part;
EXPLAIN
INSERT OVERWRITE TABLE ctas_part PARTITION (modkey)
SELECT key, key FROM src ORDER BY key;

INSERT OVERWRITE TABLE ctas_part PARTITION (modkey)
SELECT key, key FROM src ORDER BY key;

SELECT * FROM ctas_part;

-- vectorized | order by cast | dynamic partition cast | limit
--     n      |       y       |          n             |   n
TRUNCATE TABLE ctas_part;
EXPLAIN
INSERT OVERWRITE TABLE ctas_part PARTITION (modkey)
SELECT key, key FROM src ORDER BY cast(key as int);

INSERT OVERWRITE TABLE ctas_part PARTITION (modkey)
SELECT key, key FROM src ORDER BY cast(key as int);

SELECT * FROM ctas_part;

set hive.vectorized.execution.enabled=true;

-- vectorized | order by cast | dynamic partition cast | limit
--     y      |       n       |          y             |   y
TRUNCATE TABLE ctas_part;
EXPLAIN
INSERT OVERWRITE TABLE ctas_part PARTITION (modkey)
SELECT key, ceil(key / 100) FROM src ORDER BY key LIMIT 10;

INSERT OVERWRITE TABLE ctas_part PARTITION (modkey)
SELECT key, ceil(key / 100) FROM src ORDER BY key LIMIT 10;

SELECT * FROM ctas_part;

-- vectorized | order by cast | dynamic partition cast | limit
--     y      |       y       |          y             |   y
TRUNCATE TABLE ctas_part;
EXPLAIN
INSERT OVERWRITE TABLE ctas_part PARTITION (modkey)
SELECT key, ceil(key / 100) FROM src ORDER BY cast(key as int) LIMIT 10;

INSERT OVERWRITE TABLE ctas_part PARTITION (modkey)
SELECT key, ceil(key / 100) FROM src ORDER BY cast(key as int) LIMIT 10;

SELECT * FROM ctas_part;

-- vectorized | order by cast | dynamic partition cast | limit
--     y      |       n       |          y             |   n
TRUNCATE TABLE ctas_part;
EXPLAIN
INSERT OVERWRITE TABLE ctas_part PARTITION (modkey)
SELECT key, ceil(key / 100) FROM src ORDER BY key;

INSERT OVERWRITE TABLE ctas_part PARTITION (modkey)
SELECT key, ceil(key / 100) FROM src ORDER BY key;

SELECT * FROM ctas_part;

-- vectorized | order by cast | dynamic partition cast | limit
--     y      |       y       |          y             |   n
-- TODO: Rows within a partition should be ordered by int comparison
--       of `key` column. Here, ordering is by string comparison of
--       `key` column despite `cast(.. as int)`
TRUNCATE TABLE ctas_part;
EXPLAIN
INSERT OVERWRITE TABLE ctas_part PARTITION (modkey)
SELECT key, ceil(key / 100) FROM src ORDER BY cast(key as int);

INSERT OVERWRITE TABLE ctas_part PARTITION (modkey)
SELECT key, ceil(key / 100) FROM src ORDER BY cast(key as int);

SELECT * FROM ctas_part;

-- vectorized | order by cast | dynamic partition cast | limit
--     y      |       n       |          n             |   y
TRUNCATE TABLE ctas_part;
EXPLAIN
INSERT OVERWRITE TABLE ctas_part PARTITION (modkey)
SELECT key, key FROM src ORDER BY key LIMIT 10;

INSERT OVERWRITE TABLE ctas_part PARTITION (modkey)
SELECT key, key FROM src ORDER BY key LIMIT 10;

SELECT * FROM ctas_part;

-- vectorized | order by cast | dynamic partition cast | limit
--     y      |       y       |          n             |   y
TRUNCATE TABLE ctas_part;
EXPLAIN
INSERT OVERWRITE TABLE ctas_part PARTITION (modkey)
SELECT key, key FROM src ORDER BY cast(key as int) LIMIT 10;

INSERT OVERWRITE TABLE ctas_part PARTITION (modkey)
SELECT key, key FROM src ORDER BY cast(key as int) LIMIT 10;

SELECT * FROM ctas_part;

-- vectorized | order by cast | dynamic partition cast | limit
--     y      |       n       |          n             |   n
TRUNCATE TABLE ctas_part;
EXPLAIN
INSERT OVERWRITE TABLE ctas_part PARTITION (modkey)
SELECT key, key FROM src ORDER BY key;

INSERT OVERWRITE TABLE ctas_part PARTITION (modkey)
SELECT key, key FROM src ORDER BY key;

SELECT * FROM ctas_part;

-- vectorized | order by cast | dynamic partition cast | limit
--     y      |       y       |          n             |   n
TRUNCATE TABLE ctas_part;
EXPLAIN
INSERT OVERWRITE TABLE ctas_part PARTITION (modkey)
SELECT key, key FROM src ORDER BY cast(key as int);

INSERT OVERWRITE TABLE ctas_part PARTITION (modkey)
SELECT key, key FROM src ORDER BY cast(key as int);

SELECT * FROM ctas_part;


-- extra field in source, no limit
-- vectorized | order by cast | dynamic partition cast | limit
--     y      |       n       |          y             |   y
DROP TABLE src;
CREATE TABLE src (key string, dummy double) STORED AS TEXTFILE;

insert into src values ('2000', 0.1), ('60', 0.2), ('100', 0.3), ('5', 0.4);


TRUNCATE TABLE ctas_part;
EXPLAIN
INSERT OVERWRITE TABLE ctas_part PARTITION (modkey)
SELECT key, ceil(key / 100) FROM src ORDER BY key;

INSERT OVERWRITE TABLE ctas_part PARTITION (modkey)
SELECT key, ceil(key / 100) FROM src ORDER BY key;

SELECT * FROM ctas_part;