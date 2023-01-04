set hive.vectorized.execution.enabled=false;

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

set hive.exec.pre.hooks = org.apache.hadoop.hive.ql.hooks.EnforceReadOnlyDatabaseHook;

CREATE DATABASE writable;
CREATE TABLE writable.src
    (cint INT)
    CLUSTERED BY (cint) INTO 1 BUCKETS STORED AS ORC
    TBLPROPERTIES ('transactional'='true');
ALTER DATABASE writable SET DBPROPERTIES('readonly' = 'false');
INSERT INTO writable.src VALUES(1);

CREATE DATABASE readonly;
CREATE TABLE readonly.src
    (cint INT)
    CLUSTERED BY (cint) INTO 1 BUCKETS STORED AS ORC
    TBLPROPERTIES ('transactional'='true');
ALTER DATABASE readonly SET DBPROPERTIES('readonly' = 'true');
INSERT INTO readonly.src VALUES(1);
