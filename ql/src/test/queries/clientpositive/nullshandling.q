set hive.default.nulls.last=false;

CREATE TABLE table1 (
  k1 STRING,
  f1 STRING,
  sequence_num BIGINT,
  create_bsk BIGINT,
  change_bsk BIGINT,
  op_code STRING )
PARTITIONED BY (run_id BIGINT)
CLUSTERED BY (k1) SORTED BY (k1, change_bsk, sequence_num) INTO 4 BUCKETS
STORED AS ORC;

DESCRIBE table1;

DROP TABLE table1;

set hive.default.nulls.last=true;

CREATE TABLE table2 (
  k1 STRING,
  f1 STRING,
  sequence_num BIGINT,
  create_bsk BIGINT,
  change_bsk BIGINT,
  op_code STRING )
PARTITIONED BY (run_id BIGINT)
CLUSTERED BY (k1) SORTED BY (k1, change_bsk, sequence_num) INTO 4 BUCKETS
STORED AS ORC;

DESCRIBE table2;

DROP TABLE table2;