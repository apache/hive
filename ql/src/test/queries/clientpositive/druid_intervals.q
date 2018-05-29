set hive.druid.broker.address.default=localhost.test;

CREATE EXTERNAL TABLE druid_table_1_n0
STORED BY 'org.apache.hadoop.hive.druid.QTestDruidStorageHandler'
TBLPROPERTIES ("druid.datasource" = "wikipedia");

DESCRIBE FORMATTED druid_table_1_n0;

-- (-∞‥+∞)
EXPLAIN
SELECT `__time`
FROM druid_table_1_n0;

-- (-∞‥2012-03-01 00:00:00)
EXPLAIN
SELECT `__time`
FROM druid_table_1_n0
WHERE `__time` < '2012-03-01 00:00:00';

-- [2010-01-01 00:00:00‥2012-03-01 00:00:00)
EXPLAIN
SELECT `__time`
FROM druid_table_1_n0
WHERE `__time` >= '2010-01-01 00:00:00' AND `__time` <= '2012-03-01 00:00:00';

-- [2010-01-01 00:00:00‥2011-01-01 00:00:00)
EXPLAIN
SELECT `__time`
FROM druid_table_1_n0
WHERE `__time` >= '2010-01-01 00:00:00' AND `__time` <= '2012-03-01 00:00:00'
    AND `__time` < '2011-01-01 00:00:00';

-- [2010-01-01 00:00:00‥2011-01-01 00:00:00]
EXPLAIN
SELECT `__time`
FROM druid_table_1_n0
WHERE `__time` BETWEEN '2010-01-01 00:00:00' AND '2011-01-01 00:00:00';

-- [2010-01-01 00:00:00‥2011-01-01 00:00:00],[2012-01-01 00:00:00‥2013-01-01 00:00:00]
EXPLAIN
SELECT `__time`
FROM druid_table_1_n0
WHERE (`__time` BETWEEN '2010-01-01 00:00:00' AND '2011-01-01 00:00:00')
    OR (`__time` BETWEEN '2012-01-01 00:00:00' AND '2013-01-01 00:00:00');

-- OVERLAP [2010-01-01 00:00:00‥2012-01-01 00:00:00]
EXPLAIN
SELECT `__time`
FROM druid_table_1_n0
WHERE (`__time` BETWEEN '2010-01-01 00:00:00' AND '2011-01-01 00:00:00')
    OR (`__time` BETWEEN '2010-06-01 00:00:00' AND '2012-01-01 00:00:00');

-- IN: MULTIPLE INTERVALS [2010-01-01 00:00:00‥2010-01-01 00:00:00),[2011-01-01 00:00:00‥2011-01-01 00:00:00)
EXPLAIN
SELECT `__time`
FROM druid_table_1_n0
WHERE `__time` IN ('2010-01-01 00:00:00','2011-01-01 00:00:00');

EXPLAIN
SELECT `__time`, robot
FROM druid_table_1_n0
WHERE robot = 'user1' AND `__time` IN ('2010-01-01 00:00:00','2011-01-01 00:00:00');

EXPLAIN
SELECT `__time`, robot
FROM druid_table_1_n0
WHERE robot = 'user1' OR `__time` IN ('2010-01-01 00:00:00','2011-01-01 00:00:00');
