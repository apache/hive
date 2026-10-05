set hive.mapred.mode=nonstrict;

DROP TABLE IF EXISTS union_src_today;
DROP TABLE IF EXISTS union_src_prev;
DROP TABLE IF EXISTS union_output;

CREATE TABLE union_src_today (id INT, val STRING) STORED AS TEXTFILE;
CREATE TABLE union_src_prev (id INT, val STRING) STORED AS TEXTFILE;

CREATE TABLE union_output (id INT, val STRING)
PARTITIONED BY (run_dt STRING)
STORED AS TEXTFILE;

-- -----------------------------------------------------------------------
-- Scenario A: Both branches produce rows
-- prev has rows 1,2,3 — today has rows 3,4 — anti-join passes rows 1,2
-- Branch 1 (anti-join): rows from prev NOT in today → ids 1,2
-- Branch 2 (today): all rows from today → ids 3,4
-- -----------------------------------------------------------------------
INSERT INTO union_src_prev VALUES (1,'aaa'), (2,'bbb'), (3,'ccc');
INSERT INTO union_src_today VALUES (3,'ccc_new'), (4,'ddd');

INSERT OVERWRITE TABLE union_output PARTITION (run_dt='2024-01-02')
SELECT p.id, p.val
FROM union_src_prev p
LEFT OUTER JOIN union_src_today t ON p.id = t.id
WHERE t.id IS NULL
UNION ALL
SELECT id, val
FROM union_src_today;

-- Partition must contain exactly the output rows (ids 1,2,3,4)
SELECT id, val FROM union_output WHERE run_dt='2024-01-02' ORDER BY id;

-- Partition directory must NOT contain any -tmp.*.moved directories
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/union_output/run_dt=2024-01-02/;

-- -----------------------------------------------------------------------
-- Scenario B: First branch produces 0 rows (all prev rows exist in today)
-- prev has rows 3,4 — today has rows 3,4 — anti-join returns nothing
-- Branch 1 (anti-join): 0 rows ← triggers the bug
-- Branch 2 (today): rows 3,4
-- -----------------------------------------------------------------------
TRUNCATE TABLE union_src_prev;
TRUNCATE TABLE union_src_today;

INSERT INTO union_src_prev VALUES (3,'ccc'), (4,'ddd');
INSERT INTO union_src_today VALUES (3,'ccc_new'), (4,'ddd_new');

INSERT OVERWRITE TABLE union_output PARTITION (run_dt='2024-01-03')
SELECT p.id, p.val
FROM union_src_prev p
LEFT OUTER JOIN union_src_today t ON p.id = t.id
WHERE t.id IS NULL
UNION ALL
SELECT id, val
FROM union_src_today;

-- Partition must contain only Branch 2 rows (ids 3,4)
SELECT id, val FROM union_output WHERE run_dt='2024-01-03' ORDER BY id;

-- Partition directory must NOT contain any -tmp.*.moved directories.
-- Before the fix, -tmp.HIVE_UNION_SUBDIR_1.moved would appear here.
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/union_output/run_dt=2024-01-03/;

-- with flatten subdirectories enabled
set hive.tez.union.flatten.subdirectories=true;

INSERT OVERWRITE TABLE union_output PARTITION (run_dt='2024-01-03')
SELECT p.id, p.val
FROM union_src_prev p
LEFT OUTER JOIN union_src_today t ON p.id = t.id
WHERE t.id IS NULL
UNION ALL
SELECT id, val
FROM union_src_today;

-- Partition must contain only Branch 2 rows (ids 3,4)
SELECT id, val FROM union_output WHERE run_dt='2024-01-03' ORDER BY id;

-- Partition directory must NOT contain any -tmp.*.moved directories.
-- Before the fix, -tmp.HIVE_UNION_SUBDIR_1.moved would appear here.
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/union_output/run_dt=2024-01-03/;

DROP TABLE IF EXISTS union_src_today;
DROP TABLE IF EXISTS union_src_prev;
DROP TABLE IF EXISTS union_output;