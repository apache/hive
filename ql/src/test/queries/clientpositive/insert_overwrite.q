set hive.exec.dynamic.partition.mode=nonstrict;

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

