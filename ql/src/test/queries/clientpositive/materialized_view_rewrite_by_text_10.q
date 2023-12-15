-- Materialzed view definition has non-deterministic function
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

CREATE TABLE EMPS (ENAME STRING, BIRTH_EPOCH_SECS INT) STORED AS ORC TBLPROPERTIES ('transactional'='true');

INSERT INTO EMPS
VALUES ('Victor', UNIX_TIMESTAMP()),
       ('Alex', UNIX_TIMESTAMP() + 2),
       ('Bob', UNIX_TIMESTAMP() + 5),
       ('Alice', UNIX_TIMESTAMP() + 10);

CREATE MATERIALIZED VIEW v_emp AS SELECT * FROM EMPS WHERE BIRTH_EPOCH_SECS <= UNIX_TIMESTAMP();

-- View can not be used
explain cbo
SELECT * FROM EMPS WHERE BIRTH_EPOCH_SECS <= UNIX_TIMESTAMP();
SELECT * FROM EMPS WHERE BIRTH_EPOCH_SECS <= UNIX_TIMESTAMP();
