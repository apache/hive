--! qt:dataset:src
SET hive.mapred.mode=nonstrict;
SET hive.vectorized.execution.enabled=true;
SET hive.optimize.topnkey=true;

set hive.auto.convert.join=true;
SET hive.optimize.ppd=true;
SET hive.ppd.remove.duplicatefilters=true;
SET hive.tez.dynamic.partition.pruning=true;
SET hive.optimize.metadataonly=false;
SET hive.optimize.index.filter=true;
SET hive.tez.min.bloom.filter.entries=1;

SET hive.stats.fetch.column.stats=true;
SET hive.cbo.enable=true;

SET hive.optimize.topnkey=true;

EXPLAIN
SELECT /*+ MAPJOIN(a) */ * FROM src a LEFT OUTER JOIN src b on a.key=b.key ORDER BY a.key LIMIT 5;