-- EXCLUDE_HADOOP_MAJOR_VERSIONS(0.20)

CREATE TABLE repairtable(col STRING) PARTITIONED BY (p1 STRING, p2 STRING);

MSCK TABLE repairtable;

dfs -mkdir -p ../build/ql/test/data/warehouse/repairtable/p1=a/p2=a;
dfs -mkdir -p ../build/ql/test/data/warehouse/repairtable/p1=b/p2=a;

MSCK TABLE repairtable;

MSCK REPAIR TABLE repairtable;

MSCK TABLE repairtable;


