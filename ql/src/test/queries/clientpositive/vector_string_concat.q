SET hive.vectorized.execution.enabled=true;

DROP TABLE over1k;
DROP TABLE over1korc;

-- data setup
CREATE TABLE over1k(t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
           ts timestamp,
           dec decimal(4,2),
           bin binary)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/over1k' OVERWRITE INTO TABLE over1k;

CREATE TABLE over1korc(t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
           ts timestamp,
           dec decimal(4,2),
           bin binary)
STORED AS ORC;

INSERT INTO TABLE over1korc SELECT * FROM over1k;

EXPLAIN SELECT s AS `string`,
       CONCAT(CONCAT('      ',s),'      ') AS `none_padded_str`,
       CONCAT(CONCAT('|',RTRIM(CONCAT(CONCAT('      ',s),'      '))),'|') AS `none_z_rtrim_str`
       FROM over1korc LIMIT 20;

SELECT s AS `string`,
       CONCAT(CONCAT('      ',s),'      ') AS `none_padded_str`,
       CONCAT(CONCAT('|',RTRIM(CONCAT(CONCAT('      ',s),'      '))),'|') AS `none_z_rtrim_str`
       FROM over1korc LIMIT 20;

------------------------------------------------------------------------------------------

create table vectortab2k(
            t tinyint,
            si smallint,
            i int,
            b bigint,
            f float,
            d double,
            dc decimal(38,18),
            bo boolean,
            s string,
            s2 string,
            ts timestamp,
            ts2 timestamp,
            dt date)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/vectortab2k' OVERWRITE INTO TABLE vectortab2k;

create table vectortab2korc(
            t tinyint,
            si smallint,
            i int,
            b bigint,
            f float,
            d double,
            dc decimal(38,18),
            bo boolean,
            s string,
            s2 string,
            ts timestamp,
            ts2 timestamp,
            dt date)
STORED AS ORC;

INSERT INTO TABLE vectortab2korc SELECT * FROM vectortab2k;

EXPLAIN
SELECT CONCAT(CONCAT(CONCAT('Quarter ',CAST(CAST((MONTH(dt) - 1) / 3 + 1 AS INT) AS STRING)),'-'),CAST(YEAR(dt) AS STRING)) AS `field`
    FROM vectortab2korc 
    GROUP BY CONCAT(CONCAT(CONCAT('Quarter ',CAST(CAST((MONTH(dt) - 1) / 3 + 1 AS INT) AS STRING)),'-'),CAST(YEAR(dt) AS STRING))
    ORDER BY `field`
    LIMIT 50;

SELECT CONCAT(CONCAT(CONCAT('Quarter ',CAST(CAST((MONTH(dt) - 1) / 3 + 1 AS INT) AS STRING)),'-'),CAST(YEAR(dt) AS STRING)) AS `field`
    FROM vectortab2korc 
    GROUP BY CONCAT(CONCAT(CONCAT('Quarter ',CAST(CAST((MONTH(dt) - 1) / 3 + 1 AS INT) AS STRING)),'-'),CAST(YEAR(dt) AS STRING))
    ORDER BY `field`
    LIMIT 50;
