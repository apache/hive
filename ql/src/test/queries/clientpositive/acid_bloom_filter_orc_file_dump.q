SET hive.vectorized.execution.enabled=FALSE;
SET hive.mapred.mode=nonstrict;

SET hive.support.concurrency=TRUE;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

DROP TABLE if exists bloomTest;

CREATE TABLE bloomTest(
  msisdn  STRING,
  imsi    VARCHAR(20),
  imei    BIGINT,
  cell_id BIGINT)

ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'

TBLPROPERTIES (
  'bucketing_version'='2',
  'orc.bloom.filter.columns'='msisdn,cell_id,imsi',
  'orc.bloom.filter.fpp'='0.02',
  'transactional'='true'
);

INSERT INTO bloomTest VALUES ('12345', '12345', 12345, 12345);
INSERT INTO bloomTest VALUES ('2345', '2345', 2345, 2345);

SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecOrcFileDump;
SELECT * FROM bloomTest LIMIT 1;