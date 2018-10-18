--! qt:dataset:srcpart
--! qt:dataset:lineitem
set hive.stats.dbclass=fs;
set hive.stats.autogather=true;
set hive.cbo.enable=false;

DROP TABLE IF EXISTS lineitem_ix_n1;
CREATE TABLE lineitem_ix_n1 (L_ORDERKEY      INT,
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

LOAD DATA LOCAL INPATH '../../data/files/lineitem.txt' OVERWRITE INTO TABLE lineitem_ix_n1;

CREATE INDEX lineitem_ix_lshipdate_idx ON TABLE lineitem_ix_n1(l_shipdate) AS 'org.apache.hadoop.hive.ql.index.AggregateIndexHandler' WITH DEFERRED REBUILD IDXPROPERTIES("AGGREGATES"="count(l_shipdate)");
ALTER INDEX lineitem_ix_lshipdate_idx ON lineitem_ix_n1 REBUILD;

explain select l_shipdate, count(l_shipdate)
from lineitem_ix_n1
group by l_shipdate;

select l_shipdate, count(l_shipdate)
from lineitem_ix_n1
group by l_shipdate
order by l_shipdate;

set hive.optimize.index.groupby=true;

explain select l_shipdate, count(l_shipdate)
from lineitem_ix_n1
group by l_shipdate;

select l_shipdate, count(l_shipdate)
from lineitem_ix_n1
group by l_shipdate
order by l_shipdate;

set hive.optimize.index.groupby=false;


explain select year(l_shipdate) as year,
        month(l_shipdate) as month,
        count(l_shipdate) as monthly_shipments
from lineitem_ix_n1
group by year(l_shipdate), month(l_shipdate) 
order by year, month;

select year(l_shipdate) as year,
        month(l_shipdate) as month,
        count(l_shipdate) as monthly_shipments
from lineitem_ix_n1
group by year(l_shipdate), month(l_shipdate) 
order by year, month;

set hive.optimize.index.groupby=true;

explain select year(l_shipdate) as year,
        month(l_shipdate) as month,
        count(l_shipdate) as monthly_shipments
from lineitem_ix_n1
group by year(l_shipdate), month(l_shipdate) 
order by year, month;

select year(l_shipdate) as year,
        month(l_shipdate) as month,
        count(l_shipdate) as monthly_shipments
from lineitem_ix_n1
group by year(l_shipdate), month(l_shipdate) 
order by year, month;

explain select lastyear.month,
        thisyear.month,
        (thisyear.monthly_shipments - lastyear.monthly_shipments) /
lastyear.monthly_shipments as monthly_shipments_delta
   from (select year(l_shipdate) as year,
                month(l_shipdate) as month,
                count(l_shipdate) as monthly_shipments
           from lineitem_ix_n1
          where year(l_shipdate) = 1997
          group by year(l_shipdate), month(l_shipdate)
        )  lastyear join
        (select year(l_shipdate) as year,
                month(l_shipdate) as month,
                count(l_shipdate) as monthly_shipments
           from lineitem_ix_n1
          where year(l_shipdate) = 1998
          group by year(l_shipdate), month(l_shipdate)
        )  thisyear
  on lastyear.month = thisyear.month;

explain  select l_shipdate, cnt
from (select l_shipdate, count(l_shipdate) as cnt from lineitem_ix_n1 group by l_shipdate
union all
select l_shipdate, l_orderkey as cnt
from lineitem_ix_n1) dummy;

CREATE TABLE tbl_n2(key int, value int);
CREATE INDEX tbl_key_idx ON TABLE tbl_n2(key) AS 'org.apache.hadoop.hive.ql.index.AggregateIndexHandler' WITH DEFERRED REBUILD IDXPROPERTIES("AGGREGATES"="count(key)");
ALTER INDEX tbl_key_idx ON tbl_n2 REBUILD;

EXPLAIN select key, count(key) from tbl_n2 where key = 1 group by key;
EXPLAIN select key, count(key) from tbl_n2 group by key;

EXPLAIN select count(1) from tbl_n2;
EXPLAIN select count(key) from tbl_n2;

EXPLAIN select key FROM tbl_n2 GROUP BY key;
EXPLAIN select key FROM tbl_n2 GROUP BY value, key;
EXPLAIN select key FROM tbl_n2 WHERE key = 3 GROUP BY key;
EXPLAIN select key FROM tbl_n2 WHERE value = 2 GROUP BY key;
EXPLAIN select key FROM tbl_n2 GROUP BY key, substr(key,2,3);

EXPLAIN select key, value FROM tbl_n2 GROUP BY value, key;
EXPLAIN select key, value FROM tbl_n2 WHERE value = 1 GROUP BY key, value;

EXPLAIN select DISTINCT key FROM tbl_n2;
EXPLAIN select DISTINCT key FROM tbl_n2;
EXPLAIN select DISTINCT key FROM tbl_n2;
EXPLAIN select DISTINCT key, value FROM tbl_n2;
EXPLAIN select DISTINCT key, value FROM tbl_n2 WHERE value = 2;
EXPLAIN select DISTINCT key, value FROM tbl_n2 WHERE value = 2 AND key = 3;
EXPLAIN select DISTINCT key, value FROM tbl_n2 WHERE value = key;
EXPLAIN select DISTINCT key, substr(value,2,3) FROM tbl_n2 WHERE value = key;
EXPLAIN select DISTINCT key, substr(value,2,3) FROM tbl_n2;

EXPLAIN select * FROM (select DISTINCT key, value FROM tbl_n2) v1 WHERE v1.value = 2;

DROP TABLE tbl_n2;

CREATE TABLE tblpart_n0 (key int, value string) PARTITIONED BY (ds string, hr int);
INSERT OVERWRITE TABLE tblpart_n0 PARTITION (ds='2008-04-08', hr=11) SELECT key, value FROM srcpart WHERE ds = '2008-04-08' AND hr = 11;
INSERT OVERWRITE TABLE tblpart_n0 PARTITION (ds='2008-04-08', hr=12) SELECT key, value FROM srcpart WHERE ds = '2008-04-08' AND hr = 12;
INSERT OVERWRITE TABLE tblpart_n0 PARTITION (ds='2008-04-09', hr=11) SELECT key, value FROM srcpart WHERE ds = '2008-04-09' AND hr = 11;
INSERT OVERWRITE TABLE tblpart_n0 PARTITION (ds='2008-04-09', hr=12) SELECT key, value FROM srcpart WHERE ds = '2008-04-09' AND hr = 12;

CREATE INDEX tbl_part_index ON TABLE tblpart_n0(key) AS 'org.apache.hadoop.hive.ql.index.AggregateIndexHandler' WITH DEFERRED REBUILD IDXPROPERTIES("AGGREGATES"="count(key)");

ALTER INDEX tbl_part_index ON tblpart_n0 PARTITION (ds='2008-04-08', hr=11) REBUILD;
EXPLAIN SELECT key, count(key) FROM tblpart_n0 WHERE ds='2008-04-09' AND hr=12 AND key < 10 GROUP BY key;

ALTER INDEX tbl_part_index ON tblpart_n0 PARTITION (ds='2008-04-08', hr=12) REBUILD;
ALTER INDEX tbl_part_index ON tblpart_n0 PARTITION (ds='2008-04-09', hr=11) REBUILD;
ALTER INDEX tbl_part_index ON tblpart_n0 PARTITION (ds='2008-04-09', hr=12) REBUILD;
EXPLAIN SELECT key, count(key) FROM tblpart_n0 WHERE ds='2008-04-09' AND hr=12 AND key < 10 GROUP BY key;

DROP INDEX tbl_part_index on tblpart_n0;
DROP TABLE tblpart_n0;

CREATE TABLE tbl_n2(key int, value int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'; 
LOAD DATA LOCAL INPATH '../../data/files/tbl.txt' OVERWRITE INTO TABLE tbl_n2;

CREATE INDEX tbl_key_idx ON TABLE tbl_n2(key) AS 'org.apache.hadoop.hive.ql.index.AggregateIndexHandler' WITH DEFERRED REBUILD IDXPROPERTIES("AGGREGATES"="count(key)");
ALTER INDEX tbl_key_idx ON tbl_n2 REBUILD;

set hive.optimize.index.groupby=false;
explain select key, count(key) from tbl_n2 group by key order by key;
select key, count(key) from tbl_n2 group by key order by key;
set hive.optimize.index.groupby=true;
explain select key, count(key) from tbl_n2 group by key order by key;
select key, count(key) from tbl_n2 group by key order by key;
DROP TABLE tbl_n2;

reset hive.cbo.enable;
