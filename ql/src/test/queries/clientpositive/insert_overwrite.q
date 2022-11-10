set hive.stats.column.autogather=false;
set hive.stats.autogather=false;
set hive.compute.query.using.stats=false;

set hive.create.as.insert.only=true;
set hive.default.fileformat.managed=ORC;
set hive.strict.managed.tables=true;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

CREATE EXTERNAL TABLE ext_non_part (col string);
INSERT INTO ext_non_part VALUES ('first'), ('second');
CREATE TABLE b (col string);

INSERT OVERWRITE TABLE ext_non_part SELECT * FROM b;

-- should be 0
SELECT count(*) FROM ext_non_part;

drop table ext_non_part;

CREATE TABLE int_non_part (col string) STORED AS ORC TBLPROPERTIES ('transactional'='true');
INSERT INTO int_non_part VALUES ('first'), ('second');

INSERT OVERWRITE TABLE int_non_part SELECT * FROM b;

-- should be 0
SELECT count(*) FROM int_non_part;

drop table int_non_part;


CREATE TABLE int_non_part (col string);
INSERT INTO int_non_part VALUES ('first'), ('second');

INSERT OVERWRITE TABLE int_non_part SELECT * FROM b;

-- should be 0
SELECT count(*) FROM int_non_part;

drop table int_non_part;

CREATE TABLE int_buck (col string)
CLUSTERED BY (col) INTO 4 BUCKETS;

INSERT INTO int_buck VALUES ('first'), ('second'), ('third'), ('fourth');

-- should be 4
SELECT count(*) FROM int_buck;

INSERT OVERWRITE TABLE int_buck SELECT col FROM b;

-- should be 0
SELECT count(*) FROM int_buck;
SELECT * FROM int_buck;

drop table int_buck;
drop table b;


CREATE EXTERNAL TABLE ext_part (col string) partitioned by (par string);
INSERT INTO ext_part PARTITION (par='1') VALUES ('first'), ('second');
INSERT INTO ext_part PARTITION (par='2') VALUES ('first'), ('second');
CREATE TABLE b (par string, col string);

INSERT OVERWRITE TABLE ext_part PARTITION (par) SELECT * FROM b;

-- should be 4
SELECT count(*) FROM ext_part;

INSERT INTO b VALUES ('third', '1');

INSERT OVERWRITE TABLE ext_part PARTITION (par) SELECT * FROM b;

-- should be 3
SELECT count(*) FROM ext_part;

SELECT * FROM ext_part ORDER BY par, col;

-- removing a partition manually should not fail the next insert overwrite operation
dfs -rm -r ${hiveconf:hive.metastore.warehouse.dir}/ext_part/par=1;
INSERT OVERWRITE TABLE ext_part PARTITION (par) SELECT * FROM b;

drop table ext_part;
drop table b;

CREATE TABLE int_part (col string) partitioned by (par string);
INSERT INTO int_part PARTITION (par='1') VALUES ('first'), ('second');
INSERT INTO int_part PARTITION (par='2') VALUES ('first'), ('second');
INSERT INTO int_part PARTITION (par='3') VALUES ('first'), ('second');
CREATE TABLE b (par string, col string);

INSERT OVERWRITE TABLE int_part PARTITION (par) SELECT * FROM b;

-- should be 6
SELECT count(*) FROM int_part;

INSERT OVERWRITE TABLE int_part PARTITION (par='3') SELECT col FROM b;

-- should be 4
SELECT count(*) FROM int_part;

INSERT INTO b VALUES ('third', '1');

INSERT OVERWRITE TABLE int_part PARTITION (par) SELECT * FROM b;

-- should be 3
SELECT count(*) FROM int_part;

SELECT * FROM int_part ORDER BY par, col;

drop table int_part;
drop table b;
