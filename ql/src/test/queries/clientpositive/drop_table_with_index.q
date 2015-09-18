set hive.stats.dbclass=fs;
set hive.stats.autogather=true;
set hive.cbo.enable=true;

DROP TABLE IF EXISTS aa;
CREATE TABLE aa (L_ORDERKEY      INT,
                                L_PARTKEY       INT,
                                L_SUPPKEY       INT,
                                L_LINENUMBER    INT,
                                L_QUANTITY      DOUBLE,
                                L_EXTENDEDPRICE DOUBLE,
                                L_DISCOUNT      DOUBLE,
                                L_TAX           DOUBLE,
                                L_RETURNFLAG    STRING,
                                L_LINESTATUS    STRING,
                                l_shipdate      STRING,
                                L_COMMITDATE    STRING,
                                L_RECEIPTDATE   STRING,
                                L_SHIPINSTRUCT  STRING,
                                L_SHIPMODE      STRING,
                                L_COMMENT       STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|';

LOAD DATA LOCAL INPATH '../../data/files/lineitem.txt' OVERWRITE INTO TABLE aa;

CREATE INDEX aa_lshipdate_idx ON TABLE aa(l_shipdate) AS 'org.apache.hadoop.hive.ql.index.AggregateIndexHandler' WITH DEFERRED REBUILD IDXPROPERTIES("AGGREGATES"="count(l_shipdate)");
ALTER INDEX aa_lshipdate_idx ON aa REBUILD;

show tables;

explain select l_shipdate, count(l_shipdate)
from aa
group by l_shipdate;

