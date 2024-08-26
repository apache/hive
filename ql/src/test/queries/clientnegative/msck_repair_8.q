DROP TABLE IF EXISTS repairtable;

CREATE TABLE repairtable(col STRING) PARTITIONED BY (p1 STRING);

MSCK REPAIR TABLE default.repairtable.p1;