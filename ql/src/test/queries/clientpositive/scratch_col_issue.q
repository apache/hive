
set hive.fetch.task.conversion=none;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.vectorized.execution.enabled=true;
set hive.vectorized.reuse.scratch.columns=true;
set hive.auto.convert.join=true;

CREATE EXTERNAL TABLE scratch_col_issue_txt(
  `id` int,
  `value` string,
  `date_string` string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n'
LOCATION '../../data/files/scratch_col_issue_test_data';

CREATE TABLE scratch_col_issue
AS SELECT * FROM scratch_col_issue_txt;

DESCRIBE FORMATTED scratch_col_issue;

EXPLAIN VECTORIZATION DETAIL SELECT
  CASE WHEN scratch_col_issue.value in (
    'TermDeposit', 'RecurringDeposit',
    'CertificateOfDeposit'
  ) THEN NVL(
    (
      from_unixtime(
        unix_timestamp(
          cast(scratch_col_issue.date_string as date)
        ),
        'MM-dd-yyyy'
      )
    ),
    ' '
  ) ELSE '' END AS MAT_DTE
FROM
  scratch_col_issue
WHERE
  NVL(scratch_col_issue.id, '') IN (
      SELECT
        EXPLODE(
          SPLIT('8800', ',')
        )
  );

SELECT
  CASE WHEN scratch_col_issue.value in (
    'TermDeposit', 'RecurringDeposit',
    'CertificateOfDeposit'
  ) THEN NVL(
    (
      from_unixtime(
        unix_timestamp(
          cast(scratch_col_issue.date_string as date)
        ),
        'MM-dd-yyyy'
      )
    ),
    ' '
  ) ELSE '' END AS MAT_DTE
FROM
  scratch_col_issue
WHERE
  NVL(scratch_col_issue.id, '') IN (
      SELECT
        EXPLODE(
          SPLIT('8800', ',')
        )
  );
