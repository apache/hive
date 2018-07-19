--! qt:dataset:src
set hive.mapred.mode=nonstrict;
CREATE TABLE npe_test_n1 (key STRING, value STRING) PARTITIONED BY (ds STRING);

INSERT OVERWRITE TABLE npe_test_n1 PARTITION(ds='2012-12-11')
SELECT src.key, src.value FROM src WHERE key < '200';

INSERT OVERWRITE TABLE npe_test_n1 PARTITION(ds='2012-12-12')
SELECT src.key, src.value FROM src WHERE key > '200';

SELECT count(*) FROM npe_test_n1;

EXPLAIN SELECT * FROM npe_test_n1 WHERE NOT ds < 2012-11-31;

SELECT count(*) FROM npe_test_n1 WHERE NOT ds < 2012-11-31;

DROP TABLE npe_test_n1;
