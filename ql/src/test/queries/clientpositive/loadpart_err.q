set hive.cli.errors.ignore=true;

CREATE TABLE loadpart1(a STRING, b STRING) PARTITIONED BY (ds STRING);

INSERT OVERWRITE TABLE loadpart1 PARTITION (ds='2009-01-01')
SELECT TRANSFORM(src.key, src.value) USING '../data/scripts/error_script' AS (tkey, tvalue)
FROM src;

DESCRIBE loadpart1;
SHOW PARTITIONS loadpart1;

LOAD DATA LOCAL INPATH '../data1/files/kv1.txt' INTO TABLE loadpart1 PARTITION(ds='2009-05-05');
SHOW PARTITIONS loadpart1;


