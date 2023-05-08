--! qt:replace:/(\s+totalSize\s+)\S+(\s+)/$1#Masked#$2/
SET hive.support.concurrency=true;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

SET metastore.strict.managed.tables=true;
SET hive.default.fileformat=textfile;
SET hive.default.fileformat.managed=orc;

SET metastore.create.as.acid=true;

CREATE TABLE cmv_basetable_n4 (a int, b varchar(256), c decimal(10,2));

INSERT INTO cmv_basetable_n4 VALUES (1, 'alfred', 10.30),(2, 'bob', 3.14),(2, 'bonnie', 172342.2),(3, 'calvin', 978.76),(3, 'charlie', 9.8);

CREATE MATERIALIZED VIEW cmv_mat_view_n4 disable rewrite
AS SELECT a, b, c FROM cmv_basetable_n4;

DESCRIBE FORMATTED cmv_mat_view_n4;
