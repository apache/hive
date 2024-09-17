--test against vectorized LLAP execution mode
-- SORT_QUERY_RESULTS
-- Mask neededVirtualColumns due to non-strict order
--! qt:replace:/(\s+neededVirtualColumns:\s)(.*)/$1#Masked#/

set hive.llap.io.enabled=true;
set hive.vectorized.execution.enabled=true;
set hive.optimize.shared.work.merge.ts.schema=true;

DROP TABLE IF EXISTS llap_orders PURGE;
DROP TABLE IF EXISTS llap_items PURGE;
DROP TABLE IF EXISTS mig_source PURGE;
DROP TABLE IF EXISTS target_ice PURGE;
DROP TABLE IF EXISTS calls PURGE;
DROP TABLE IF EXISTS display PURGE;

-- read after a merge call
CREATE EXTERNAL TABLE calls (
  s_key bigint,
  year int
) PARTITIONED BY SPEC (year)
STORED BY Iceberg STORED AS ORC
TBLPROPERTIES ('format-version'='2');

INSERT INTO calls (s_key, year) VALUES (1090969, 2022);


CREATE EXTERNAL TABLE display (
  skey bigint,
  hierarchy_number string,
  hierarchy_name string,
  language_id int,
  hierarchy_display string,
  orderby string
)
STORED BY Iceberg STORED AS ORC
TBLPROPERTIES ('format-version'='2');

INSERT INTO display (skey, language_id, hierarchy_display) VALUES
  (1090969, 3, 'f9e59bae9b131de1d8f02d887ee91e20-mergeupdated1-updated1'),
  (1090969, 3, 'f9e59bae9b131de1d8f02d887ee91e20-mergeupdated1-updated1-insertnew1');

MERGE INTO display USING (
  SELECT distinct display_skey, display, display as orig_display
  FROM (
    SELECT D.skey display_skey, D.hierarchy_display display
    FROM (
      SELECT s_key FROM calls WHERE s_key =  1090969
    ) R
    INNER JOIN display D
      ON R.s_key = D.skey AND D.language_id = 3
    GROUP BY D.skey,
      D.hierarchy_display
  ) sub1

  UNION ALL

  SELECT distinct display_skey, null as display, display as orig_display
  FROM (
    SELECT D.skey display_skey, D.hierarchy_display display
    FROM (
      SELECT s_key FROM calls WHERE s_key =  1090969
    ) R
    INNER JOIN display D
      ON R.s_key = D.skey AND D.language_id = 3
    GROUP BY D.skey,
      D.hierarchy_display
  ) sub2
) sub
ON display.skey = sub.display_skey
    and display.hierarchy_display = sub.display

WHEN MATCHED THEN
  UPDATE SET hierarchy_display = concat(sub.display, '-mergeupdated1')
WHEN NOT MATCHED THEN
  INSERT (skey, language_id, hierarchy_display) values (sub.display_skey, 3, concat(sub.orig_display, '-mergenew1'));

SELECT * FROM display;

-- try read after a delete query
CREATE EXTERNAL TABLE target_ice(a int, b string, c int)  STORED BY ICEBERG STORED AS ORC tblproperties ('format-version'='2');
INSERT INTO target_ice values (1, 'one', 50);
DELETE FROM target_ice WHERE a = 1;
SELECT * FROM target_ice;

CREATE EXTERNAL TABLE llap_items (itemid INT, price INT, category STRING, name STRING, description STRING) STORED BY ICEBERG STORED AS ORC;
INSERT INTO llap_items VALUES
(0, 35000,  'Sedan',     'Model 3', 'Standard range plus'),
(1, 45000,  'Sedan',     'Model 3', 'Long range'),
(2, 50000,  'Sedan',     'Model 3', 'Performance'),
(3, 48000,  'Crossover', 'Model Y', 'Long range'),
(4, 55000,  'Crossover', 'Model Y', 'Performance'),
(5, 83000,  'Sports',    'Model S', 'Long range'),
(6, 123000, 'Sports',   'Model S', 'Plaid');

CREATE EXTERNAL TABLE llap_orders (orderid INT, quantity INT, itemid INT, tradets TIMESTAMP) PARTITIONED BY (p1 STRING, P2 STRING) STORED BY ICEBERG STORED AS ORC;
INSERT INTO llap_orders VALUES
(0, 48, 5, timestamp('2000-06-04 19:55:46.129'), 'EU', 'DE'),
(1, 12, 6, timestamp('2007-06-24 19:23:22.829'), 'US', 'TX'),
(2, 76, 4, timestamp('2018-02-19 23:43:51.995'), 'EU', 'DE'),
(3, 91, 5, timestamp('2000-07-15 09:09:11.587'), 'US', 'NJ'),
(4, 18, 6, timestamp('2007-12-02 22:30:39.302'), 'EU', 'ES'),
(5, 71, 5, timestamp('2010-02-08 20:31:23.430'), 'EU', 'DE'),
(6, 78, 3, timestamp('2016-02-22 20:37:37.025'), 'EU', 'FR'),
(7, 88, 0, timestamp('2020-03-26 18:47:40.611'), 'EU', 'FR'),
(8, 87, 4, timestamp('2003-02-20 00:48:09.139'), 'EU', 'ES'),
(9, 60, 6, timestamp('2012-08-28 01:35:54.283'), 'EU', 'IT'),
(10, 24, 5, timestamp('2015-03-28 18:57:50.069'), 'US', 'NY'),
(11, 42, 2, timestamp('2012-06-27 01:13:32.350'), 'EU', 'UK'),
(12, 37, 4, timestamp('2020-08-09 01:18:50.153'), 'US', 'NY'),
(13, 52, 1, timestamp('2019-09-04 01:46:19.558'), 'EU', 'UK'),
(14, 96, 3, timestamp('2019-03-05 22:00:03.020'), 'US', 'NJ'),
(15, 18, 3, timestamp('2001-09-11 00:14:12.687'), 'EU', 'FR'),
(16, 46, 0, timestamp('2013-08-31 02:16:17.878'), 'EU', 'UK'),
(17, 26, 5, timestamp('2001-02-01 20:05:32.317'), 'EU', 'FR'),
(18, 68, 5, timestamp('2009-12-29 08:44:08.048'), 'EU', 'ES'),
(19, 54, 6, timestamp('2015-08-15 01:59:22.177'), 'EU', 'HU'),
(20, 10, 0, timestamp('2018-05-06 12:56:12.789'), 'US', 'CA');

--verify row level filtering works with Iceberg ORC too
set hive.auto.convert.join=true;
set hive.disable.unsafe.external.table.operations=false;
set hive.vectorized.execution.mapjoin.native.fast.hashtable.enabled=true;

explain select sum(quantity)
    from llap_orders o, llap_items i
    where
        o.itemid = i.itemid and i.price != 83000 and
        (
            (o.quantity > 0 and o.quantity < 39)
                or
            (o.quantity > 39 and o.quantity < 69)
                or
            (o.quantity > 70 )
        );
select sum(quantity)
from llap_orders o, llap_items i
where
    o.itemid = i.itemid and i.price != 83000 and
    (
        (o.quantity > 0 and o.quantity < 39)
            or
        (o.quantity > 39 and o.quantity < 69)
            or
        (o.quantity > 70 )
    );

--select query without any schema change yet
SELECT i.name, i.description, SUM(o.quantity) FROM llap_items i JOIN llap_orders o ON i.itemid = o.itemid  WHERE p1 = 'EU' and i.price >= 50000 GROUP BY i.name, i.description;


--schema evolution on unpartitioned table
--renames and reorders
ALTER TABLE llap_items CHANGE category cat string AFTER description;
ALTER TABLE llap_items CHANGE price cost int AFTER name;
SELECT i.name, i.description, SUM(o.quantity) FROM llap_items i JOIN llap_orders o ON i.itemid = o.itemid  WHERE p1 = 'EU' and i.cost >= 100000 GROUP BY i.name, i.description;

--adding a column
ALTER TABLE llap_items ADD COLUMNS (to60 float);
INSERT INTO llap_items VALUES
(7, 'Model X', 93000, 'Long range', 'SUV', 3.8),
(7, 'Model X', 113000, 'Plaid', 'SUV', 2.5);
SELECT cat, min(to60) from llap_items group by cat;

--removing a column
ALTER TABLE llap_items REPLACE COLUMNS (itemid int, name string, cost int, description string, to60 float);
INSERT INTO llap_items VALUES
(8, 'Cybertruck', 40000, 'Single Motor RWD', 6.5),
(9, 'Cybertruck', 50000, 'Dual Motor AWD', 4.5);
SELECT name, min(to60), max(cost) FROM llap_items WHERE itemid > 3 GROUP BY name;


--schema evolution on partitioned table (including partition changes)
--renames and reorders
ALTER TABLE llap_orders CHANGE tradets ordertime timestamp AFTER P2;
ALTER TABLE llap_orders CHANGE p1 region string;
INSERT INTO llap_orders VALUES
(21, 21, 8, 'EU', 'HU', timestamp('2000-01-04 19:55:46.129'));
SELECT region, min(ordertime), sum(quantity) FROM llap_orders WHERE itemid > 5 GROUP BY region;

ALTER TABLE llap_orders CHANGE P2 state string;
SELECT region, state, min(ordertime), sum(quantity) FROM llap_orders WHERE itemid > 5 GROUP BY region, state;

--adding new column
ALTER TABLE llap_orders ADD COLUMNS (city string);
INSERT INTO llap_orders VALUES
(22, 99, 9, 'EU', 'DE', timestamp('2021-01-04 19:55:46.129'), 'München');
SELECT state, max(city) from llap_orders WHERE region = 'EU' GROUP BY state;

--making it a partition column
ALTER TABLE llap_orders SET PARTITION SPEC (region, state, city);
INSERT INTO llap_orders VALUES
(23, 89, 6, 'EU', 'IT', timestamp('2021-02-04 19:55:46.129'), 'Venezia');
SELECT state, max(city), avg(itemid) from llap_orders WHERE region = 'EU' GROUP BY state;

--de-partitioning a column
ALTER TABLE llap_orders SET PARTITION SPEC (state, city);
INSERT INTO llap_orders VALUES
(24, 88, 5, 'EU', 'UK', timestamp('2006-02-04 19:55:46.129'), 'London');
SELECT state, max(city), avg(itemid) from llap_orders WHERE region = 'EU' GROUP BY state;

--removing a column from schema
ALTER TABLE llap_orders REPLACE COLUMNS (quantity int, itemid int, region string, state string, ordertime timestamp, city string);
INSERT INTO llap_orders VALUES
(88, 5, 'EU', 'FR', timestamp('2006-02-04 19:55:46.129'), 'Paris');
SELECT state, max(city), avg(itemid) from llap_orders WHERE region = 'EU' GROUP BY state;


--some more projections
SELECT o.city, i.name, min(i.cost), max(to60), sum(o.quantity) FROM llap_items i JOIN llap_orders o ON i.itemid = o.itemid  WHERE region = 'EU' and i.cost >= 50000 and ordertime > timestamp('2010-01-01') GROUP BY o.city, i.name;
SELECT i.name, i.description, SUM(o.quantity) FROM llap_items i JOIN llap_orders o ON i.itemid = o.itemid  WHERE region = 'EU' and i.cost >= 50000 GROUP BY i.name, i.description;

---------------------------------------------
--Test migrated partitioned table gets cached

CREATE EXTERNAL TABLE mig_source (id int) partitioned by (region string) stored as ORC;
INSERT INTO mig_source VALUES (1, 'EU'), (1, 'US'), (2, 'EU'), (3, 'EU'), (2, 'US');
ALTER TABLE mig_source CONVERT TO ICEBERG;

-- Should miss, but fill cache
SELECT region, SUM(id) from mig_source GROUP BY region;

-- Should hit cache
set hive.llap.io.cache.only=true;
SELECT region, SUM(id) from mig_source GROUP BY region;
set hive.llap.io.cache.only=false;
