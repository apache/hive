CREATE EXTERNAL TABLE default.hive_branch_bug
( foo BIGINT )
STORED BY ICEBERG;

ALTER TABLE default.hive_branch_bug CREATE BRANCH empty;

INSERT INTO default.hive_branch_bug (foo) VALUES (1), (2), (3), (4);

SELECT COUNT(*)
FROM default.hive_branch_bug
UNION ALL
SELECT COUNT(*)
FROM default.hive_branch_bug.branch_empty;

SELECT COUNT(*)
FROM default.hive_branch_bug.branch_empty
UNION ALL
SELECT COUNT(*)
FROM default.hive_branch_bug;

SELECT
(SELECT COUNT(*) FROM default.hive_branch_bug),
(SELECT COUNT(*) FROM default.hive_branch_bug.branch_empty);

 SELECT COUNT(*)
FROM default.hive_branch_bug
WHERE foo > 0
UNION ALL
SELECT COUNT(*)
FROM default.hive_branch_bug.branch_empty
WHERE foo > 0;

SELECT *
FROM (
SELECT COUNT(*) c
FROM default.hive_branch_bug
) a
UNION ALL
SELECT *
FROM (
SELECT COUNT(*) c
FROM default.hive_branch_bug.branch_empty
) b;