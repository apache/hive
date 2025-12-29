-- Mask random uuid
--! qt:replace:/(\s+'uuid'=')\S+('\s*)/$1#Masked#$2/
--! qt:replace:/(\s+uuid\s+)\S+/$1#Masked#/
-- Mask random snapshot id
--! qt:replace:/('current-snapshot-id'=')\d+/$1#SnapshotId#/
-- Mask current-snapshot-timestamp-ms
--! qt:replace:/('current-snapshot-timestamp-ms'=')\d+/$1#Masked#/
-- Mask iceberg version
--! qt:replace:/("iceberg-version":")(\w+\s\w+\s\d+\.\d+\.\d+\s\(\w+\s\w+\))/$1#Masked#/
-- Mask added-files-size
--! qt:replace:/(\S\"added-files-size":")(\d+)(")/$1#Masked#$3/
-- Mask total-files-size
--! qt:replace:/(\S\"total-files-size":")(\d+)(")/$1#Masked#$3/

CREATE TABLE t (
  ts_us     timestamp,
  ts_ns     nanosecond timestamp,
  ts_tz_us  timestamp with local time zone,
  ts_tz_ns  nanosecond timestamp with local time zone
)
STORED BY ICEBERG
TBLPROPERTIES ('format-version'='3');

SHOW CREATE TABLE t;
DESCRIBE t;

INSERT INTO t VALUES (
  '2025-12-18 10:15:30.123456789',
  '2025-12-18 10:15:30.123456789',
  '2025-12-18 10:15:30.123456789',
  '2025-12-18 10:15:30.123456789'
);

SELECT ts_ns FROM t ORDER BY ts_ns;
SELECT ts_tz_ns FROM t ORDER BY ts_tz_ns;

SELECT * FROM t;

CREATE TABLE tgt STORED BY ICEBERG TBLPROPERTIES ('format-version'='3') AS SELECT * FROM t;

SHOW CREATE TABLE tgt;
DESCRIBE tgt;

SELECT * FROM tgt;
