--! qt:dataset:src,srcpart:ONLY
set hive.support.quoted.identifiers=none;
EXPLAIN 
SHOW TABLE EXTENDED IN default LIKE `src`;

SHOW TABLE EXTENDED IN default LIKE `src`;

SHOW TABLE EXTENDED from default LIKE `src`;

SHOW TABLE EXTENDED LIKE `src`;

SHOW TABLE EXTENDED LIKE `src_`;

SHOW TABLE EXTENDED from default LIKE `src_`;

SHOW TABLE EXTENDED LIKE "%";

SHOW TABLE EXTENDED from default LIKE "%";

SHOW TABLE EXTENDED LIKE `srcpart` PARTITION(ds='2008-04-08', hr=11);

-- no PARTITION clause on a partitioned table: must list all of its partitions
SHOW TABLE EXTENDED LIKE `srcpart`;

SHOW TABLE EXTENDED from default LIKE src;
