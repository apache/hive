CREATE TABLE t (
  ts_us     timestamp,
  ts_ns     nanosecond timestamp,
  ts_tz_us  timestamp with local time zone,
  ts_tz_ns  nanosecond timestamp with local time zone
)
STORED BY ICEBERG
TBLPROPERTIES ('format-version'='3');

INSERT INTO t VALUES (
  '2025-12-18 10:15:30.123456789',
  '2025-12-18 10:15:30.123456789',
  '2025-12-18 10:15:30.123456789',
  '2025-12-18 10:15:30.123456789'
);

SELECT * FROM t;

CREATE TABLE tgt STORED BY ICEBERG TBLPROPERTIES ('format-version'='3') AS SELECT * FROM t;

select * from tgt;