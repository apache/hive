set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

set hive.query.results.cache.enabled=true;
set hive.query.results.cache.nontransactional.tables.enabled=true;
set hive.fetch.task.conversion=none;

CREATE TABLE author (fname STRING) STORED AS ORC;
INSERT INTO author VALUES ('Victor');
SELECT fname FROM author;

DROP TABLE author;

CREATE TABLE author (fname STRING) STORED AS ORC;
INSERT INTO author VALUES ('Alexander');
SELECT fname FROM author;
