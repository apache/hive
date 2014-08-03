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
