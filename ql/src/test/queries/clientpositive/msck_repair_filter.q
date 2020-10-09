SET metastore.expression.proxy=org.apache.hadoop.hive.ql.optimizer.ppr.PartitionExpressionForMetastore;
SET metastore.decode.filter.expression.tostring=true;

DROP TABLE IF EXISTS repairtable_filter;

CREATE TABLE repairtable_filter(col STRING) PARTITIONED BY (p1 STRING, p2 STRING);


dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_filter/p1=c/p2=a;
dfs -touchz ${system:test.local.warehouse.dir}/repairtable_filter/p1=c/p2=a/datafile;

dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_filter/p1=b/p2=a;
dfs -touchz ${system:test.local.warehouse.dir}/repairtable_filter/p1=b/p2=a/datafile;


-- recover all the partition
MSCK REPAIR TABLE default.repairtable_filter SYNC PARTITIONS;

show partitions repairtable_filter;

-- Test Single Filter Operator "="
dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_filter/p1=b/p2=e;
dfs -touchz ${system:test.local.warehouse.dir}/repairtable_filter/p1=b/p2=e/datafile;

dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_filter/p1=c/p2=e;
dfs -touchz ${system:test.local.warehouse.dir}/repairtable_filter/p1=c/p2=e/datafile;

MSCK REPAIR TABLE repairtable_filter SYNC PARTITIONS (p1 = 'b');
show partitions repairtable_filter;

-- Test Single Filter Operator "like"
dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_filter/p1=ca/p2=e;
dfs -touchz ${system:test.local.warehouse.dir}/repairtable_filter/p1=ca/p2=e/datafile;

dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_filter/p1=cb/p2=e;
dfs -touchz ${system:test.local.warehouse.dir}/repairtable_filter/p1=cb/p2=e/datafile;

dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_filter/p1=bb/p2=e;
dfs -touchz ${system:test.local.warehouse.dir}/repairtable_filter/p1=bb/p2=e/datafile;

MSCK REPAIR TABLE default.repairtable_filter SYNC PARTITIONS (p1 LIKE 'c%');

show partitions repairtable_filter;

-- Test Single Filter Operator "!="
dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_filter/p1=bba/p2=e;
dfs -touchz ${system:test.local.warehouse.dir}/repairtable_filter/p1=bba/p2=e/datafile;

dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_filter/p1=bbc/p2=e;
dfs -touchz ${system:test.local.warehouse.dir}/repairtable_filter/p1=bbc/p2=e/datafile;

MSCK REPAIR TABLE default.repairtable_filter SYNC PARTITIONS (p1 != 'bba');
show partitions repairtable_filter;

-- Test Multiple Filter Operator "="
dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_filter/p1=ber/p2=ehb;
dfs -touchz ${system:test.local.warehouse.dir}/repairtable_filter/p1=b/p2=e/datafile;

dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_filter/p1=cp/p2=eg;
dfs -touchz ${system:test.local.warehouse.dir}/repairtable_filter/p1=c/p2=e/datafile;

MSCK REPAIR TABLE default.repairtable_filter SYNC PARTITIONS (p1 = 'ber', p2 = 'ehb');
show partitions repairtable_filter;

-- Test Multiple Filter Operator "like"
dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_filter/p1=cab/p2=eb;
dfs -touchz ${system:test.local.warehouse.dir}/repairtable_filter/p1=ca/p2=e/datafile;

dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_filter/p1=cob/p2=es;
dfs -touchz ${system:test.local.warehouse.dir}/repairtable_filter/p1=cb/p2=e/datafile;

dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_filter/p1=bb/p2=ep;
dfs -touchz ${system:test.local.warehouse.dir}/repairtable_filter/p1=bb/p2=e/datafile;

MSCK REPAIR TABLE default.repairtable_filter SYNC PARTITIONS (p1 like 'c%', p2 like 'e%');
show partitions repairtable_filter;

-- Test Filter Operator "!="
dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_filter/p1=bba/p2=e;
dfs -touchz ${system:test.local.warehouse.dir}/repairtable_filter/p1=bba/p2=e/datafile;

dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_filter/p1=bbc/p2=e;
dfs -touchz ${system:test.local.warehouse.dir}/repairtable_filter/p1=bbc/p2=e/datafile;

MSCK REPAIR TABLE default.repairtable_filter SYNC PARTITIONS (p1!='bba', p2 != 'e');
show partitions repairtable_filter;

-- Test Multiple Filter Operator ">"
dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_filter/p1=bbqa/p2=eq;
dfs -touchz ${system:test.local.warehouse.dir}/repairtable_filter/p1=bbqa/p2=eq/datafile;

dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_filter/p1=bwbc/p2=eq;
dfs -touchz ${system:test.local.warehouse.dir}/repairtable_filter/p1=bwbc/p2=eq/datafile;

MSCK REPAIR TABLE default.repairtable_filter SYNC PARTITIONS (p1 > 'bba');
show partitions repairtable_filter;

-- Test Multiple Filter Operator "<"
MSCK REPAIR TABLE default.repairtable_filter SYNC PARTITIONS (p1 < 'bwbc');
show partitions repairtable_filter;

-- Test Multiple Filter Operator ">="
MSCK REPAIR TABLE default.repairtable_filter SYNC PARTITIONS (p1 >= 'bwbc');
show partitions repairtable_filter;

-- Test Multiple Filter Operator "<="
MSCK REPAIR TABLE default.repairtable_filter SYNC PARTITIONS (p1 <= 'bbqa');
show partitions repairtable_filter;

-- Test Multiple Filter With Multiple Operators
dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_filter/p1=bter/p2=ehb;
dfs -touchz ${system:test.local.warehouse.dir}/repairtable_filter/p1=bter/p2=ehb/datafile;

dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_filter/p1=cpa/p2=eg;
dfs -touchz ${system:test.local.warehouse.dir}/repairtable_filter/p1=cpa/p2=eg/datafile;

MSCK REPAIR TABLE default.repairtable_filter SYNC PARTITIONS (p1='ber', p2!='ehb');
show partitions repairtable_filter;

dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_filter/p1=cab/p2=eb;
dfs -touchz ${system:test.local.warehouse.dir}/repairtable_filter/p1=cab/p2=eb/datafile;

dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_filter/p1=cobf/p2=ess;
dfs -touchz ${system:test.local.warehouse.dir}/repairtable_filter/p1=cobf/p2=ess/datafile;

dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_filter/p1=bbe/p2=ep;
dfs -touchz ${system:test.local.warehouse.dir}/repairtable_filter/p1=bbe/p2=ep/datafile;

MSCK REPAIR TABLE default.repairtable_filter SYNC PARTITIONS (p1 like 'c%', p2='e');
show partitions repairtable_filter;


dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_filter/p1=bbw/p2=e;
dfs -touchz ${system:test.local.warehouse.dir}/repairtable_filter/p1=bbw/p2=e/datafile;

dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_filter/p1=bbsc/p2=e;
dfs -touchz ${system:test.local.warehouse.dir}/repairtable_filter/p1=bbsc/p2=e/datafile;

MSCK REPAIR TABLE default.repairtable_filter SYNC PARTITIONS (p1!='bba', p2 like 'e%');
show partitions repairtable_filter;

-- Test DROP PARTITIONS

dfs -rmr ${system:test.local.warehouse.dir}/repairtable_filter/p1=bbsc;
MSCK REPAIR TABLE default.repairtable_filter SYNC PARTITIONS (p1='bbsc');
show partitions repairtable_filter;

dfs -rmr ${system:test.local.warehouse.dir}/repairtable_filter/p1=bbw;
MSCK REPAIR TABLE default.repairtable_filter SYNC PARTITIONS (p1 like 'b%');
show partitions repairtable_filter;

dfs -rmr ${system:test.local.warehouse.dir}/repairtable_filter/p1=bba;
MSCK REPAIR TABLE default.repairtable_filter SYNC PARTITIONS (p1 like 'bb%');
show partitions repairtable_filter;

DROP TABLE repairtable_filter;