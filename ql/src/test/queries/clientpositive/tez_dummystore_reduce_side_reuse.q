set hive.mapred.mode=nonstrict;
set hive.auto.convert.join=false;
set hive.auto.convert.sortmerge.join=true;
set hive.auto.convert.sortmerge.join.reduce.side=true;
set hive.auto.convert.sortmerge.join.to.mapjoin=false;
set hive.optimize.bucketmapjoin=true;
-- Force multiple reduce tasks for the SAME vertex, and inflate each task's requested
-- container memory so only a couple of containers fit in the (small, test-sized)
-- NodeManager at once. This forces the Tez AM to sequentially REUSE the same container
-- for multiple tasks of the Reducer vertex within this single query - the ObjectCache
-- entry (keyed by vertex name, scoped per-query) then hands the second task the SAME
-- already-initialized operator instances the first task used, exercising
-- TezDummyStoreOperator's wiring cleanup on closeOp().
set mapred.reduce.tasks=8;
set hive.exec.reducers.max=8;
set hive.tez.container.size=2048;
set hive.tez.java.opts=-Xmx1600m;

CREATE TABLE dummystore_src (id int, val string, sort_col int);

INSERT INTO dummystore_src VALUES
  (1,'A',3),(1,'A',1),(1,'A',2),
  (2,'B',1),(2,'B',3),(2,'B',2),
  (3,'C',2),(3,'C',1),
  (4,'D',1),(4,'D',3),(4,'D',2),
  (5,'E',1),(5,'E',3),(5,'E',2),
  (6,'F',2),(6,'F',1),
  (7,'G',1),(7,'G',3),(7,'G',2),
  (8,'H',1),(8,'H',3),(8,'H',2);

-- Reduce-side merge join with PTF (ROW_NUMBER) + LEFT OUTER JOIN: one side aggregates via
-- GROUP BY, the other picks the "latest" row per id via ROW_NUMBER. Both sides shuffle into
-- a Reducer vertex containing a DummyStore + CommonMergeJoinOperator pipeline. With 8
-- reducer tasks contending for a couple of containers, some containers must sequentially
-- process more than one task of this vertex, exercising TezDummyStoreOperator's wiring
-- cleanup on closeOp() and ReduceRecordProcessor's close ordering for the last group in
-- each task's merge-scan order.
EXPLAIN
SELECT b.id, b.max_val, e.enrch_val
FROM
  (SELECT id, max(val) AS max_val FROM dummystore_src GROUP BY id) b
  LEFT JOIN
  (SELECT id, val AS enrch_val
   FROM (SELECT id, val, sort_col,
                ROW_NUMBER() OVER (PARTITION BY id ORDER BY sort_col DESC) AS rn
         FROM dummystore_src) t
   WHERE rn = 1) e
  ON b.id = e.id;

-- SORT_QUERY_RESULTS
SELECT b.id, b.max_val, e.enrch_val
FROM
  (SELECT id, max(val) AS max_val FROM dummystore_src GROUP BY id) b
  LEFT JOIN
  (SELECT id, val AS enrch_val
   FROM (SELECT id, val, sort_col,
                ROW_NUMBER() OVER (PARTITION BY id ORDER BY sort_col DESC) AS rn
         FROM dummystore_src) t
   WHERE rn = 1) e
  ON b.id = e.id;

DROP TABLE dummystore_src;
