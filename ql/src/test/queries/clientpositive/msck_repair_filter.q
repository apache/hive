DROP TABLE IF EXISTS repairtable;

CREATE TABLE repairtable(col STRING) PARTITIONED BY (p1 STRING, p2 STRING);


dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable/p1=c/p2=a;
dfs -touchz ${system:test.warehouse.dir}/repairtable/p1=c/p2=a/datafile;

dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable/p1=b/p2=a;
dfs -touchz ${system:test.warehouse.dir}/repairtable/p1=b/p2=a/datafile;


-- recover all the partition
MSCK REPAIR TABLE default.repairtable SYNC PARTITIONS;

select * from repairtable;

-- Test Single Filter Operator "="
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable/p1=b/p2=e;
dfs -touchz ${system:test.warehouse.dir}/repairtable/p1=b/p2=e/datafile;

dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable/p1=c/p2=e;
dfs -touchz ${system:test.warehouse.dir}/repairtable/p1=c/p2=e/datafile;

MSCK REPAIR TABLE default.repairtable SYNC PARTITIONS WHERE p1='b';
select * from repairtable;

-- Test Single Filter Operator "like"
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable/p1=ca/p2=e;
dfs -touchz ${system:test.warehouse.dir}/repairtable/p1=ca/p2=e/datafile;

dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable/p1=cb/p2=e;
dfs -touchz ${system:test.warehouse.dir}/repairtable/p1=cb/p2=e/datafile;

dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable/p1=bb/p2=e;
dfs -touchz ${system:test.warehouse.dir}/repairtable/p1=bb/p2=e/datafile;

MSCK REPAIR TABLE default.repairtable SYNC PARTITIONS WHERE p1 like 'c%';

select * from repairtable;


-- Test Single Filter Operator "!="
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable/p1=bba/p2=e;
dfs -touchz ${system:test.warehouse.dir}/repairtable/p1=bba/p2=e/datafile;

dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable/p1=bbc/p2=e;
dfs -touchz ${system:test.warehouse.dir}/repairtable/p1=bbc/p2=e/datafile;

-- recover specific partition with like operator
MSCK REPAIR TABLE default.repairtable SYNC PARTITIONS WHERE p1!='bba';

select * from repairtable;


-- Test Multiple Filter Operator "="
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable/p1=ber/p2=ehb;
dfs -touchz ${system:test.warehouse.dir}/repairtable/p1=b/p2=e/datafile;

dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable/p1=cp/p2=eg;
dfs -touchz ${system:test.warehouse.dir}/repairtable/p1=c/p2=e/datafile;

MSCK REPAIR TABLE default.repairtable SYNC PARTITIONS WHERE p1='ber' and p2='ehb';
select * from repairtable;

-- Test Multiple Filter Operator "like"
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable/p1=cab/p2=eb;
dfs -touchz ${system:test.warehouse.dir}/repairtable/p1=ca/p2=e/datafile;

dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable/p1=cob/p2=es;
dfs -touchz ${system:test.warehouse.dir}/repairtable/p1=cb/p2=e/datafile;

dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable/p1=bb/p2=ep;
dfs -touchz ${system:test.warehouse.dir}/repairtable/p1=bb/p2=e/datafile;

MSCK REPAIR TABLE default.repairtable SYNC PARTITIONS WHERE p1 like 'c%' and p2 like 'e%';

select * from repairtable;


-- Test Multiple Filter Operator "!="
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable/p1=bba/p2=e;
dfs -touchz ${system:test.warehouse.dir}/repairtable/p1=bba/p2=e/datafile;

dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable/p1=bbc/p2=e;
dfs -touchz ${system:test.warehouse.dir}/repairtable/p1=bbc/p2=e/datafile;

-- recover specific partition with like operator
MSCK REPAIR TABLE default.repairtable SYNC PARTITIONS WHERE p1!='bba' and p2!='e';

select * from repairtable;


-- Test Multiple Filter With Multiple Operators
dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable/p1=bter/p2=ehb;
dfs -touchz ${system:test.warehouse.dir}/repairtable/p1=bter/p2=ehb/datafile;

dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable/p1=cpa/p2=eg;
dfs -touchz ${system:test.warehouse.dir}/repairtable/p1=cpa/p2=eg/datafile;

MSCK REPAIR TABLE default.repairtable SYNC PARTITIONS WHERE p1='ber' and p2!='ehb';
select * from repairtable;

dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable/p1=cab/p2=eb;
dfs -touchz ${system:test.warehouse.dir}/repairtable/p1=cab/p2=eb/datafile;

dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable/p1=cobf/p2=ess;
dfs -touchz ${system:test.warehouse.dir}/repairtable/p1=cobf/p2=ess/datafile;

dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable/p1=bbe/p2=ep;
dfs -touchz ${system:test.warehouse.dir}/repairtable/p1=bbe/p2=ep/datafile;

MSCK REPAIR TABLE default.repairtable SYNC PARTITIONS WHERE p1 like 'c%' and p2='e';

select * from repairtable;


dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable/p1=bbw/p2=e;
dfs -touchz ${system:test.warehouse.dir}/repairtable/p1=bbw/p2=e/datafile;

dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable/p1=bbsc/p2=e;
dfs -touchz ${system:test.warehouse.dir}/repairtable/p1=bbsc/p2=e/datafile;

-- recover specific partition with like operator
MSCK REPAIR TABLE default.repairtable SYNC PARTITIONS WHERE p1!='bba' and p2 like 'e%';

select * from repairtable;




