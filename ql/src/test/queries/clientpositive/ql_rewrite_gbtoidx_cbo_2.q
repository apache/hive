set hive.stats.dbclass=fs;
set hive.stats.autogather=true;
set hive.cbo.enable=true;
set hive.optimize.index.groupby=true;

DROP TABLE IF EXISTS lineitem_ix;
DROP INDEX IF EXISTS lineitem_ix_L_ORDERKEY_idx on lineitem_ix;
DROP INDEX IF EXISTS lineitem_ix_L_PARTKEY_idx on lineitem_ix;


CREATE TABLE lineitem_ix (L_ORDERKEY      INT,
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

LOAD DATA LOCAL INPATH '../../data/files/lineitem.txt' OVERWRITE INTO TABLE lineitem_ix;

CREATE INDEX lineitem_ix_L_ORDERKEY_idx ON TABLE lineitem_ix(L_ORDERKEY) AS 'org.apache.hadoop.hive.ql.index.AggregateIndexHandler' WITH DEFERRED REBUILD IDXPROPERTIES("AGGREGATES"="count(L_ORDERKEY)");
ALTER INDEX lineitem_ix_L_ORDERKEY_idx ON lineitem_ix REBUILD;

CREATE INDEX lineitem_ix_L_PARTKEY_idx ON TABLE lineitem_ix(L_PARTKEY) AS 'org.apache.hadoop.hive.ql.index.AggregateIndexHandler' WITH DEFERRED REBUILD IDXPROPERTIES("AGGREGATES"="count(L_PARTKEY)");
ALTER INDEX lineitem_ix_L_PARTKEY_idx ON lineitem_ix REBUILD;

explain
select count(1)
from lineitem_ix;

select count(1)
from lineitem_ix;

explain
select count(L_ORDERKEY)
from lineitem_ix;

select count(L_ORDERKEY)
from lineitem_ix;

explain select L_ORDERKEY+L_PARTKEY as keysum,
count(L_ORDERKEY), count(L_PARTKEY)
from lineitem_ix
group by L_ORDERKEY, L_PARTKEY;

select L_ORDERKEY+L_PARTKEY as keysum,
count(L_ORDERKEY), count(L_PARTKEY)
from lineitem_ix
group by L_ORDERKEY, L_PARTKEY;

explain
select L_ORDERKEY, count(L_ORDERKEY)
from  lineitem_ix
where L_ORDERKEY = 7
group by L_ORDERKEY;

select L_ORDERKEY, count(L_ORDERKEY)
from  lineitem_ix
where L_ORDERKEY = 7
group by L_ORDERKEY;

explain
select L_ORDERKEY, count(1)
from lineitem_ix
group by L_ORDERKEY;

select L_ORDERKEY, count(1)
from lineitem_ix
group by L_ORDERKEY;

explain
select count(L_ORDERKEY+1)
from lineitem_ix;

select count(L_ORDERKEY+1)
from lineitem_ix;

explain
select L_ORDERKEY, count(L_ORDERKEY+1)
from lineitem_ix
group by L_ORDERKEY;

select L_ORDERKEY, count(L_ORDERKEY+1)
from lineitem_ix
group by L_ORDERKEY;

explain
select L_ORDERKEY, count(L_ORDERKEY+1+L_ORDERKEY+2)
from lineitem_ix
group by L_ORDERKEY;

select L_ORDERKEY, count(L_ORDERKEY+1+L_ORDERKEY+2)
from lineitem_ix
group by L_ORDERKEY;

explain
select L_ORDERKEY, count(1+L_ORDERKEY+2)
from lineitem_ix
group by L_ORDERKEY;

select L_ORDERKEY, count(1+L_ORDERKEY+2)
from lineitem_ix
group by L_ORDERKEY;


explain
select L_ORDERKEY as a, count(1) as b
from lineitem_ix
where L_ORDERKEY < 7
group by L_ORDERKEY;

select L_ORDERKEY as a, count(1) as b
from lineitem_ix
where L_ORDERKEY < 7
group by L_ORDERKEY;

explain
select L_ORDERKEY, count(keysum), sum(keysum)
from
(select L_ORDERKEY, L_ORDERKEY+L_PARTKEY as keysum from lineitem_ix) tabA
group by L_ORDERKEY;

select L_ORDERKEY, count(keysum), sum(keysum)
from
(select L_ORDERKEY, L_ORDERKEY+L_PARTKEY as keysum from lineitem_ix) tabA
group by L_ORDERKEY;


explain
select L_ORDERKEY, count(L_ORDERKEY), sum(L_ORDERKEY)
from lineitem_ix
group by L_ORDERKEY;

select L_ORDERKEY, count(L_ORDERKEY), sum(L_ORDERKEY)
from lineitem_ix
group by L_ORDERKEY;

explain
select colA, count(colA)
from (select L_ORDERKEY as colA from lineitem_ix) tabA
group by colA;

select colA, count(colA)
from (select L_ORDERKEY as colA from lineitem_ix) tabA
group by colA;

explain
select keysum, count(keysum)
from
(select L_ORDERKEY+L_PARTKEY as keysum from lineitem_ix) tabA
group by keysum;

select keysum, count(keysum)
from
(select L_ORDERKEY+L_PARTKEY as keysum from lineitem_ix) tabA
group by keysum;

explain
select keysum, count(keysum)
from
(select L_ORDERKEY+1 as keysum from lineitem_ix) tabA
group by keysum;

select keysum, count(keysum)
from
(select L_ORDERKEY+1 as keysum from lineitem_ix) tabA
group by keysum;


explain
select keysum, count(1)
from
(select L_ORDERKEY+1 as keysum from lineitem_ix) tabA
group by keysum;

select keysum, count(1)
from
(select L_ORDERKEY+1 as keysum from lineitem_ix) tabA
group by keysum;


explain
select keysum, count(keysum)
from
(select L_ORDERKEY+1 as keysum from lineitem_ix where L_ORDERKEY = 7) tabA
group by keysum;

select keysum, count(keysum)
from
(select L_ORDERKEY+1 as keysum from lineitem_ix where L_ORDERKEY = 7) tabA
group by keysum;


explain
select ckeysum, count(ckeysum)
from
(select keysum, count(keysum) as ckeysum
from 
	(select L_ORDERKEY+1 as keysum from lineitem_ix where L_ORDERKEY = 7) tabA
group by keysum) tabB
group by ckeysum;

select ckeysum, count(ckeysum)
from
(select keysum, count(keysum) as ckeysum
from 
	(select L_ORDERKEY+1 as keysum from lineitem_ix where L_ORDERKEY = 7) tabA
group by keysum) tabB
group by ckeysum;

explain
select keysum, count(keysum) as ckeysum
from
(select L_ORDERKEY, count(L_ORDERKEY) as keysum
from lineitem_ix
where L_ORDERKEY < 7
group by L_ORDERKEY)tabA
group by keysum;

select keysum, count(keysum) as ckeysum
from
(select L_ORDERKEY, count(L_ORDERKEY) as keysum
from lineitem_ix
where L_ORDERKEY < 7
group by L_ORDERKEY)tabA
group by keysum;


DROP INDEX IF EXISTS src_key_idx on src;
CREATE INDEX src_key_idx ON TABLE src(key) AS 'org.apache.hadoop.hive.ql.index.AggregateIndexHandler' WITH DEFERRED REBUILD IDXPROPERTIES("AGGREGATES"="count(key)");
ALTER INDEX src_key_idx ON src REBUILD;

explain
select tabA.a, tabA.b, tabB.a, tabB.b
from
(select L_ORDERKEY as a, count(L_ORDERKEY) as b
from lineitem_ix
where L_ORDERKEY < 7
group by L_ORDERKEY) tabA
join
(select key as a, count(key) as b
from src
group by key
) tabB
on (tabA.b=tabB.b);

select tabA.a, tabA.b, tabB.a, tabB.b
from
(select L_ORDERKEY as a, count(L_ORDERKEY) as b
from lineitem_ix
where L_ORDERKEY < 7
group by L_ORDERKEY) tabA
join
(select key as a, count(key) as b
from src
group by key
) tabB
on (tabA.b=tabB.b);


explain
select tabA.a, tabA.b, tabB.a, tabB.b
from
(select L_ORDERKEY as a, count(L_ORDERKEY) as b
from lineitem_ix
where L_ORDERKEY < 7
group by L_ORDERKEY) tabA
join
(select key as a, count(key) as b
from src
group by key
) tabB
on (tabA.b=tabB.b and tabB.a < '2');

select tabA.a, tabA.b, tabB.a, tabB.b
from 
(select L_ORDERKEY as a, count(L_ORDERKEY) as b
from lineitem_ix
where L_ORDERKEY < 7
group by L_ORDERKEY) tabA
join
(select key as a, count(key) as b
from src
group by key
) tabB
on (tabA.b=tabB.b and tabB.a < '2');

EXPLAIN
select L_ORDERKEY FROM lineitem_ix GROUP BY L_ORDERKEY, L_ORDERKEY+1;

select L_ORDERKEY FROM lineitem_ix GROUP BY L_ORDERKEY, L_ORDERKEY+1;

EXPLAIN
select L_ORDERKEY, L_ORDERKEY+1, count(L_ORDERKEY) FROM lineitem_ix GROUP BY L_ORDERKEY, L_ORDERKEY+1;

select L_ORDERKEY, L_ORDERKEY+1, count(L_ORDERKEY) FROM lineitem_ix GROUP BY L_ORDERKEY, L_ORDERKEY+1;

EXPLAIN
select L_ORDERKEY+2, count(L_ORDERKEY) FROM lineitem_ix GROUP BY L_ORDERKEY+2;

select L_ORDERKEY+2, count(L_ORDERKEY) FROM lineitem_ix GROUP BY L_ORDERKEY+2;

--with cbo on, the following query can use idx

explain
select b, count(b) as ckeysum
from
(
select L_ORDERKEY as a, count(L_ORDERKEY) as b
from lineitem_ix
where L_ORDERKEY < 7
group by L_ORDERKEY
union all
select L_PARTKEY as a, count(L_PARTKEY) as b
from lineitem_ix
where L_PARTKEY < 10
group by L_PARTKEY
) tabA
group by b;

select b, count(b) as ckeysum
from
(
select L_ORDERKEY as a, count(L_ORDERKEY) as b
from lineitem_ix
where L_ORDERKEY < 7
group by L_ORDERKEY
union all
select L_PARTKEY as a, count(L_PARTKEY) as b
from lineitem_ix 
where L_PARTKEY < 10
group by L_PARTKEY
) tabA
group by b;

--with cbo on, the following query can not use idx because AggFunc is empty here

explain
select a, count(a) as ckeysum
from
(
select L_ORDERKEY as a, count(L_ORDERKEY) as b 
from lineitem_ix
where L_ORDERKEY < 7 
group by L_ORDERKEY
union all
select L_PARTKEY as a, count(L_PARTKEY) as b
from lineitem_ix
where L_PARTKEY < 10
group by L_PARTKEY
) tabA
group by a;

select a, count(a) as ckeysum
from
(
select L_ORDERKEY as a, count(L_ORDERKEY) as b
from lineitem_ix
where L_ORDERKEY < 7
group by L_ORDERKEY
union all
select L_PARTKEY as a, count(L_PARTKEY) as b
from lineitem_ix 
where L_PARTKEY < 10
group by L_PARTKEY
) tabA
group by a;

explain
select a, count(a)
from (
select case L_ORDERKEY when null then 1 else 1 END as a
from lineitem_ix)tab
group by a;

select a, count(a)
from (
select case L_ORDERKEY when null then 1 else 1 END as a
from lineitem_ix)tab
group by a;

