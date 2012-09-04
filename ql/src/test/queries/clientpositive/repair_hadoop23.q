

CREATE TABLE repairtable(col STRING) PARTITIONED BY (p1 STRING, p2 STRING);

-- INCLUDE_HADOOP_MAJOR_VERSIONS(0.20, 0.23)
-- When you invoke the mkdir command using versions of Hadoop up to and including 0.23,
-- they behave as if you had specified the -p option,
-- *but* they don't actually support the -p option.

-- Support for the -p option first appeared in 1.0 and 2.0,
-- but they maintain backward compatibility with older versions,
-- so they let you include -p, but if you don't they still act like you did.

-- HADOOP-8551 breaks backward compatibility with 0.23 and older versions by
-- requiring you to explicitly specify -p if you require that behavior.

MSCK TABLE repairtable;

dfs -mkdir ../build/ql/test/data/warehouse/repairtable/p1=a/p2=a;
dfs -mkdir ../build/ql/test/data/warehouse/repairtable/p1=b/p2=a;

MSCK TABLE repairtable;

MSCK REPAIR TABLE repairtable;

MSCK TABLE repairtable;


