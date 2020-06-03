-- SORT_QUERY_RESULTS
--! qt:dataset:lineitem
set hive.cli.print.header=true;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.fetch.task.conversion=none;
set hive.vectorized.execution.enabled=true;

CREATE TABLE lineitem_test_txt (L_ORDERKEY      INT,
                                L_PARTKEY       INT,
                                L_SUPPKEY       INT,
                                L_LINENUMBER    INT,
                                L_QUANTITY      INT,
                                L_EXTENDEDPRICE DOUBLE,
                                L_DISCOUNT      DOUBLE,
                                L_TAX           DECIMAL(10,2),
                                L_RETURNFLAG    CHAR(1),
                                L_LINESTATUS    CHAR(1),
                                l_shipdate      DATE,
                                L_COMMITDATE    DATE,
                                L_RECEIPTDATE   DATE,
                                L_SHIPINSTRUCT  VARCHAR(20),
                                L_SHIPMODE      CHAR(10),
                                L_COMMENT       STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|';

LOAD DATA LOCAL INPATH '../../data/files/tpch/tiny/lineitem.tbl.bz2' OVERWRITE INTO TABLE lineitem_test_txt;
CREATE TABLE lineitem_test STORED AS ORC AS SELECT * FROM lineitem_test_txt;
INSERT INTO TABLE lineitem_test VALUES (NULL,NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

SET hive.vectorized.if.expr.mode=adaptor;

EXPLAIN VECTORIZATION DETAIL
SELECT
   L_QUANTITY as Quantity,
   CASE
     WHEN L_QUANTITY = 1 THEN "Single"
     WHEN L_QUANTITY = 2 THEN "Two"
     WHEN L_QUANTITY < 10 THEN "Some"
     WHEN L_QUANTITY < 100 THEN "Many"
     ELSE "Huge number" END AS Quantity_Description,
   CASE
     WHEN L_QUANTITY = 1 THEN "Single"
     WHEN L_QUANTITY = 2 THEN "Two"
     WHEN L_QUANTITY < 10 THEN "Some"
     WHEN L_QUANTITY < 100 THEN "Many"
     ELSE NULL END AS Quantity_Description_2,
   CASE
     WHEN L_QUANTITY = 1 THEN "Single"
     WHEN L_QUANTITY = 2 THEN "Two"
     WHEN L_QUANTITY < 10 THEN "Some"
     WHEN L_QUANTITY < 100 THEN NULL
     ELSE NULL END AS Quantity_Description_3,
   IF(L_SHIPMODE = "SHIP", DATE_ADD(l_shipdate, 10), DATE_ADD(l_shipdate, 5)) AS Expected_Date,
   CASE WHEN L_RETURNFLAG = "N"
            THEN l_extendedprice * (1 - l_discount)
        ELSE 0 END AS Field_1,  -- The 0 will be an integer and requires implicit casting.
   CASE WHEN L_RETURNFLAG = "N"
            THEN l_extendedprice * (1 - l_discount)
        ELSE CAST(0 AS DOUBLE) END AS Field_2,
   IF(L_SHIPINSTRUCT = "DELIVER IN PERSON", NULL, L_TAX) AS Field_3,
   IF(L_SHIPINSTRUCT = "TAKE BACK RETURN", L_TAX, NULL) AS Field_4,
   -- For the next 2 IF stmts, the 0s are integer and require implicit casting to decimal.
   IF(L_SHIPINSTRUCT = "DELIVER IN PERSON", 0, L_TAX) AS Field_5,
   IF(L_SHIPINSTRUCT = "TAKE BACK RETURN", L_TAX, 0) AS Field_6,
   IF(L_SHIPINSTRUCT = "DELIVER IN PERSON", 0BD, L_TAX) AS Field_7,
   IF(L_SHIPINSTRUCT = "TAKE BACK RETURN", L_TAX, 0BD) AS Field_8,
   IF(L_PARTKEY > 30, CAST(L_RECEIPTDATE AS TIMESTAMP), CAST(L_COMMITDATE AS TIMESTAMP)) AS Field_9,
   IF(L_SUPPKEY > 10000, DATEDIFF(L_RECEIPTDATE, L_COMMITDATE), NULL) AS Field_10,
   IF(L_SUPPKEY > 10000, NULL, DATEDIFF(L_RECEIPTDATE, L_COMMITDATE)) AS Field_11,
   IF(L_SUPPKEY % 500 > 100, DATE_ADD('2008-12-31', 1), DATE_ADD('2008-12-31', 365)) AS Field_12
FROM lineitem_test;
SELECT
   L_QUANTITY as Quantity,
   CASE
     WHEN L_QUANTITY = 1 THEN "Single"
     WHEN L_QUANTITY = 2 THEN "Two"
     WHEN L_QUANTITY < 10 THEN "Some"
     WHEN L_QUANTITY < 100 THEN "Many"
     ELSE "Huge number" END AS Quantity_Description,
   CASE
     WHEN L_QUANTITY = 1 THEN "Single"
     WHEN L_QUANTITY = 2 THEN "Two"
     WHEN L_QUANTITY < 10 THEN "Some"
     WHEN L_QUANTITY < 100 THEN "Many"
     ELSE NULL END AS Quantity_Description_2,
   CASE
     WHEN L_QUANTITY = 1 THEN "Single"
     WHEN L_QUANTITY = 2 THEN "Two"
     WHEN L_QUANTITY < 10 THEN "Some"
     WHEN L_QUANTITY < 100 THEN NULL
     ELSE NULL END AS Quantity_Description_3,
   IF(L_SHIPMODE = "SHIP", DATE_ADD(l_shipdate, 10), DATE_ADD(l_shipdate, 5)) AS Expected_Date,
   CASE WHEN L_RETURNFLAG = "N"
            THEN l_extendedprice * (1 - l_discount)
        ELSE 0 END AS Field_1,  -- The 0 will be an integer and requires implicit casting.
   CASE WHEN L_RETURNFLAG = "N"
            THEN l_extendedprice * (1 - l_discount)
        ELSE CAST(0 AS DOUBLE) END AS Field_2,
   IF(L_SHIPINSTRUCT = "DELIVER IN PERSON", NULL, L_TAX) AS Field_3,
   IF(L_SHIPINSTRUCT = "TAKE BACK RETURN", L_TAX, NULL) AS Field_4,
   -- For the next 2 IF stmts, the 0s are integer and require implicit casting to decimal.
   IF(L_SHIPINSTRUCT = "DELIVER IN PERSON", 0, L_TAX) AS Field_5,
   IF(L_SHIPINSTRUCT = "TAKE BACK RETURN", L_TAX, 0) AS Field_6,
   IF(L_SHIPINSTRUCT = "DELIVER IN PERSON", 0BD, L_TAX) AS Field_7,
   IF(L_SHIPINSTRUCT = "TAKE BACK RETURN", L_TAX, 0BD) AS Field_8,
   IF(L_PARTKEY > 30, CAST(L_RECEIPTDATE AS TIMESTAMP), CAST(L_COMMITDATE AS TIMESTAMP)) AS Field_9,
   IF(L_SUPPKEY > 10000, DATEDIFF(L_RECEIPTDATE, L_COMMITDATE), NULL) AS Field_10,
   IF(L_SUPPKEY > 10000, NULL, DATEDIFF(L_RECEIPTDATE, L_COMMITDATE)) AS Field_11,
   IF(L_SUPPKEY % 500 > 100, DATE_ADD('2008-12-31', 1), DATE_ADD('2008-12-31', 365)) AS Field_12
FROM lineitem_test;

SET hive.vectorized.if.expr.mode=good;

EXPLAIN VECTORIZATION DETAIL
SELECT
   L_QUANTITY as Quantity,
   CASE
     WHEN L_QUANTITY = 1 THEN "Single"
     WHEN L_QUANTITY = 2 THEN "Two"
     WHEN L_QUANTITY < 10 THEN "Some"
     WHEN L_QUANTITY < 100 THEN "Many"
     ELSE "Huge number" END AS Quantity_Description,
   CASE
     WHEN L_QUANTITY = 1 THEN "Single"
     WHEN L_QUANTITY = 2 THEN "Two"
     WHEN L_QUANTITY < 10 THEN "Some"
     WHEN L_QUANTITY < 100 THEN "Many"
     ELSE NULL END AS Quantity_Description_2,
   CASE
     WHEN L_QUANTITY = 1 THEN "Single"
     WHEN L_QUANTITY = 2 THEN "Two"
     WHEN L_QUANTITY < 10 THEN "Some"
     WHEN L_QUANTITY < 100 THEN NULL
     ELSE NULL END AS Quantity_Description_3,
   IF(L_SHIPMODE = "SHIP", DATE_ADD(l_shipdate, 10), DATE_ADD(l_shipdate, 5)) AS Expected_Date,
   CASE WHEN L_RETURNFLAG = "N"
            THEN l_extendedprice * (1 - l_discount)
        ELSE 0 END AS Field_1,  -- The 0 will be an integer and requires implicit casting.
   CASE WHEN L_RETURNFLAG = "N"
            THEN l_extendedprice * (1 - l_discount)
        ELSE CAST(0 AS DOUBLE) END AS Field_2,
   IF(L_SHIPINSTRUCT = "DELIVER IN PERSON", NULL, L_TAX) AS Field_3,
   IF(L_SHIPINSTRUCT = "TAKE BACK RETURN", L_TAX, NULL) AS Field_4,
   -- For the next 2 IF stmts, the 0s are integer and require implicit casting to decimal.
   IF(L_SHIPINSTRUCT = "DELIVER IN PERSON", 0, L_TAX) AS Field_5,
   IF(L_SHIPINSTRUCT = "TAKE BACK RETURN", L_TAX, 0) AS Field_6,
   IF(L_SHIPINSTRUCT = "DELIVER IN PERSON", 0BD, L_TAX) AS Field_7,
   IF(L_SHIPINSTRUCT = "TAKE BACK RETURN", L_TAX, 0BD) AS Field_8,
   IF(L_PARTKEY > 30, CAST(L_RECEIPTDATE AS TIMESTAMP), CAST(L_COMMITDATE AS TIMESTAMP)) AS Field_9,
   IF(L_SUPPKEY > 10000, DATEDIFF(L_RECEIPTDATE, L_COMMITDATE), NULL) AS Field_10,
   IF(L_SUPPKEY > 10000, NULL, DATEDIFF(L_RECEIPTDATE, L_COMMITDATE)) AS Field_11,
   IF(L_SUPPKEY % 500 > 100, DATE_ADD('2008-12-31', 1), DATE_ADD('2008-12-31', 365)) AS Field_12
FROM lineitem_test;
SELECT
   L_QUANTITY as Quantity,
   CASE
     WHEN L_QUANTITY = 1 THEN "Single"
     WHEN L_QUANTITY = 2 THEN "Two"
     WHEN L_QUANTITY < 10 THEN "Some"
     WHEN L_QUANTITY < 100 THEN "Many"
     ELSE "Huge number" END AS Quantity_Description,
   CASE
     WHEN L_QUANTITY = 1 THEN "Single"
     WHEN L_QUANTITY = 2 THEN "Two"
     WHEN L_QUANTITY < 10 THEN "Some"
     WHEN L_QUANTITY < 100 THEN "Many"
     ELSE NULL END AS Quantity_Description_2,
   CASE
     WHEN L_QUANTITY = 1 THEN "Single"
     WHEN L_QUANTITY = 2 THEN "Two"
     WHEN L_QUANTITY < 10 THEN "Some"
     WHEN L_QUANTITY < 100 THEN NULL
     ELSE NULL END AS Quantity_Description_3,
   IF(L_SHIPMODE = "SHIP", DATE_ADD(l_shipdate, 10), DATE_ADD(l_shipdate, 5)) AS Expected_Date,
   CASE WHEN L_RETURNFLAG = "N"
            THEN l_extendedprice * (1 - l_discount)
        ELSE 0 END AS Field_1,  -- The 0 will be an integer and requires implicit casting.
   CASE WHEN L_RETURNFLAG = "N"
            THEN l_extendedprice * (1 - l_discount)
        ELSE CAST(0 AS DOUBLE) END AS Field_2,
   IF(L_SHIPINSTRUCT = "DELIVER IN PERSON", NULL, L_TAX) AS Field_3,
   IF(L_SHIPINSTRUCT = "TAKE BACK RETURN", L_TAX, NULL) AS Field_4,
   -- For the next 2 IF stmts, the 0s are integer and require implicit casting to decimal.
   IF(L_SHIPINSTRUCT = "DELIVER IN PERSON", 0, L_TAX) AS Field_5,
   IF(L_SHIPINSTRUCT = "TAKE BACK RETURN", L_TAX, 0) AS Field_6,
   IF(L_SHIPINSTRUCT = "DELIVER IN PERSON", 0BD, L_TAX) AS Field_7,
   IF(L_SHIPINSTRUCT = "TAKE BACK RETURN", L_TAX, 0BD) AS Field_8,
   IF(L_PARTKEY > 30, CAST(L_RECEIPTDATE AS TIMESTAMP), CAST(L_COMMITDATE AS TIMESTAMP)) AS Field_9,
   IF(L_SUPPKEY > 10000, DATEDIFF(L_RECEIPTDATE, L_COMMITDATE), NULL) AS Field_10,
   IF(L_SUPPKEY > 10000, NULL, DATEDIFF(L_RECEIPTDATE, L_COMMITDATE)) AS Field_11,
   IF(L_SUPPKEY % 500 > 100, DATE_ADD('2008-12-31', 1), DATE_ADD('2008-12-31', 365)) AS Field_12
FROM lineitem_test;

SET hive.vectorized.if.expr.mode=better;

EXPLAIN VECTORIZATION DETAIL
SELECT
   L_QUANTITY as Quantity,
   CASE
     WHEN L_QUANTITY = 1 THEN "Single"
     WHEN L_QUANTITY = 2 THEN "Two"
     WHEN L_QUANTITY < 10 THEN "Some"
     WHEN L_QUANTITY < 100 THEN "Many"
     ELSE "Huge number" END AS Quantity_Description,
   CASE
     WHEN L_QUANTITY = 1 THEN "Single"
     WHEN L_QUANTITY = 2 THEN "Two"
     WHEN L_QUANTITY < 10 THEN "Some"
     WHEN L_QUANTITY < 100 THEN "Many"
     ELSE NULL END AS Quantity_Description_2,
   CASE
     WHEN L_QUANTITY = 1 THEN "Single"
     WHEN L_QUANTITY = 2 THEN "Two"
     WHEN L_QUANTITY < 10 THEN "Some"
     WHEN L_QUANTITY < 100 THEN NULL
     ELSE NULL END AS Quantity_Description_3,
   IF(L_SHIPMODE = "SHIP", DATE_ADD(l_shipdate, 10), DATE_ADD(l_shipdate, 5)) AS Expected_Date,
   CASE WHEN L_RETURNFLAG = "N"
            THEN l_extendedprice * (1 - l_discount)
        ELSE 0 END AS Field_1,  -- The 0 will be an integer and requires implicit casting.
   CASE WHEN L_RETURNFLAG = "N"
            THEN l_extendedprice * (1 - l_discount)
        ELSE CAST(0 AS DOUBLE) END AS Field_2,
   IF(L_SHIPINSTRUCT = "DELIVER IN PERSON", NULL, L_TAX) AS Field_3,
   IF(L_SHIPINSTRUCT = "TAKE BACK RETURN", L_TAX, NULL) AS Field_4,
   -- For the next 2 IF stmts, the 0s are integer and require implicit casting to decimal.
   IF(L_SHIPINSTRUCT = "DELIVER IN PERSON", 0, L_TAX) AS Field_5,
   IF(L_SHIPINSTRUCT = "TAKE BACK RETURN", L_TAX, 0) AS Field_6,
   IF(L_SHIPINSTRUCT = "DELIVER IN PERSON", 0BD, L_TAX) AS Field_7,
   IF(L_SHIPINSTRUCT = "TAKE BACK RETURN", L_TAX, 0BD) AS Field_8,
   IF(L_PARTKEY > 30, CAST(L_RECEIPTDATE AS TIMESTAMP), CAST(L_COMMITDATE AS TIMESTAMP)) AS Field_9,
   IF(L_SUPPKEY > 10000, DATEDIFF(L_RECEIPTDATE, L_COMMITDATE), NULL) AS Field_10,
   IF(L_SUPPKEY > 10000, NULL, DATEDIFF(L_RECEIPTDATE, L_COMMITDATE)) AS Field_11,
   IF(L_SUPPKEY % 500 > 100, DATE_ADD('2008-12-31', 1), DATE_ADD('2008-12-31', 365)) AS Field_12
FROM lineitem_test;
SELECT
   L_QUANTITY as Quantity,
   CASE
     WHEN L_QUANTITY = 1 THEN "Single"
     WHEN L_QUANTITY = 2 THEN "Two"
     WHEN L_QUANTITY < 10 THEN "Some"
     WHEN L_QUANTITY < 100 THEN "Many"
     ELSE "Huge number" END AS Quantity_Description,
   CASE
     WHEN L_QUANTITY = 1 THEN "Single"
     WHEN L_QUANTITY = 2 THEN "Two"
     WHEN L_QUANTITY < 10 THEN "Some"
     WHEN L_QUANTITY < 100 THEN "Many"
     ELSE NULL END AS Quantity_Description_2,
   CASE
     WHEN L_QUANTITY = 1 THEN "Single"
     WHEN L_QUANTITY = 2 THEN "Two"
     WHEN L_QUANTITY < 10 THEN "Some"
     WHEN L_QUANTITY < 100 THEN NULL
     ELSE NULL END AS Quantity_Description_3,
   IF(L_SHIPMODE = "SHIP", DATE_ADD(l_shipdate, 10), DATE_ADD(l_shipdate, 5)) AS Expected_Date,
   CASE WHEN L_RETURNFLAG = "N"
            THEN l_extendedprice * (1 - l_discount)
        ELSE 0 END AS Field_1,  -- The 0 will be an integer and requires implicit casting.
   CASE WHEN L_RETURNFLAG = "N"
            THEN l_extendedprice * (1 - l_discount)
        ELSE CAST(0 AS DOUBLE) END AS Field_2,
   IF(L_SHIPINSTRUCT = "DELIVER IN PERSON", NULL, L_TAX) AS Field_3,
   IF(L_SHIPINSTRUCT = "TAKE BACK RETURN", L_TAX, NULL) AS Field_4,
   -- For the next 2 IF stmts, the 0s are integer and require implicit casting to decimal.
   IF(L_SHIPINSTRUCT = "DELIVER IN PERSON", 0, L_TAX) AS Field_5,
   IF(L_SHIPINSTRUCT = "TAKE BACK RETURN", L_TAX, 0) AS Field_6,
   IF(L_SHIPINSTRUCT = "DELIVER IN PERSON", 0BD, L_TAX) AS Field_7,
   IF(L_SHIPINSTRUCT = "TAKE BACK RETURN", L_TAX, 0BD) AS Field_8,
   IF(L_PARTKEY > 30, CAST(L_RECEIPTDATE AS TIMESTAMP), CAST(L_COMMITDATE AS TIMESTAMP)) AS Field_9,
   IF(L_SUPPKEY > 10000, DATEDIFF(L_RECEIPTDATE, L_COMMITDATE), NULL) AS Field_10,
   IF(L_SUPPKEY > 10000, NULL, DATEDIFF(L_RECEIPTDATE, L_COMMITDATE)) AS Field_11,
   IF(L_SUPPKEY % 500 > 100, DATE_ADD('2008-12-31', 1), DATE_ADD('2008-12-31', 365)) AS Field_12
FROM lineitem_test;
 
