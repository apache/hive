--! qt:disabled:HIVE-24816
--! qt:dataset:srcpart
--! qt:dataset:druid_table_alltypesorc
--! qt:dataset:alltypesorc

set hive.compute.query.using.stats=false;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=true;
set hive.tez.dynamic.partition.pruning=true;
set hive.tez.dynamic.semijoin.reduction=true;
set hive.optimize.metadataonly=false;
set hive.optimize.index.filter=true;
set hive.stats.autogather=true;
set hive.tez.bigtable.minsize.semijoin.reduction=1;
set hive.tez.min.bloom.filter.entries=1;
set hive.stats.fetch.column.stats=true;
set hive.disable.unsafe.external.table.operations=false;
set hive.tez.dynamic.semijoin.reduction.for.mapjoin=true;

DROP TABLE IF EXISTS alltypesorc_small;
CREATE TABLE alltypesorc_small(
    ctinyint TINYINT,
    csmallint SMALLINT,
    cint INT,
    cbigint BIGINT,
    cfloat FLOAT,
    cdouble DOUBLE,
    cstring1 STRING,
    cstring2 STRING,
    ctimestamp1 TIMESTAMP,
    cboolean1 BOOLEAN,
    cboolean2 BOOLEAN)
    STORED AS ORC;
Insert into table alltypesorc_small
Select ctinyint, csmallint, cint, cbigint, cfloat, cdouble, cstring1, cstring2, cast(`__time` as timestamp), cboolean1, cboolean2 from druid_table_alltypesorc where cstring2 like '%a%' and cstring1 like '%a%';
Select count(*) from alltypesorc_small;
Select count(*) from druid_table_alltypesorc;

DESCRIBE druid_table_alltypesorc;
DESCRIBE alltypesorc_small;

-- Test Joins on all column types one by one
-- String
set hive.disable.unsafe.external.table.operations=false;
EXPLAIN select count(*) from alltypesorc_small join druid_table_alltypesorc on (alltypesorc_small.cstring1 = druid_table_alltypesorc.cstring1);
select count(*) from alltypesorc_small join druid_table_alltypesorc on (alltypesorc_small.cstring1 = druid_table_alltypesorc.cstring1);
set hive.disable.unsafe.external.table.operations=true;
select count(*) from alltypesorc_small join druid_table_alltypesorc on (alltypesorc_small.cstring1 = druid_table_alltypesorc.cstring1);

-- tinyint
set hive.disable.unsafe.external.table.operations=false;
EXPLAIN select count(*) from alltypesorc_small join druid_table_alltypesorc on (alltypesorc_small.ctinyint = druid_table_alltypesorc.ctinyint);
select count(*) from alltypesorc_small join druid_table_alltypesorc on (alltypesorc_small.ctinyint = druid_table_alltypesorc.ctinyint);
set hive.disable.unsafe.external.table.operations=true;
select count(*) from alltypesorc_small join druid_table_alltypesorc on (alltypesorc_small.ctinyint = druid_table_alltypesorc.ctinyint);

-- smallint
set hive.disable.unsafe.external.table.operations=false;
EXPLAIN select count(*) from alltypesorc_small join druid_table_alltypesorc on (alltypesorc_small.csmallint = druid_table_alltypesorc.csmallint);
select count(*) from alltypesorc_small join druid_table_alltypesorc on (alltypesorc_small.csmallint = druid_table_alltypesorc.csmallint);
set hive.disable.unsafe.external.table.operations=true;
select count(*) from alltypesorc_small join druid_table_alltypesorc on (alltypesorc_small.csmallint = druid_table_alltypesorc.csmallint);

-- int
set hive.disable.unsafe.external.table.operations=false;
EXPLAIN select count(*) from alltypesorc_small join druid_table_alltypesorc on (alltypesorc_small.cint = druid_table_alltypesorc.cint);
select count(*) from alltypesorc_small join druid_table_alltypesorc on (alltypesorc_small.cint = druid_table_alltypesorc.cint);
set hive.disable.unsafe.external.table.operations=true;
select count(*) from alltypesorc_small join druid_table_alltypesorc on (alltypesorc_small.cint = druid_table_alltypesorc.cint);

-- bigint
set hive.disable.unsafe.external.table.operations=false;
EXPLAIN select count(*) from alltypesorc_small join druid_table_alltypesorc on (alltypesorc_small.cbigint = druid_table_alltypesorc.cbigint);
select count(*) from alltypesorc_small join druid_table_alltypesorc on (alltypesorc_small.cbigint = druid_table_alltypesorc.cbigint);
set hive.disable.unsafe.external.table.operations=true;
select count(*) from alltypesorc_small join druid_table_alltypesorc on (alltypesorc_small.cbigint = druid_table_alltypesorc.cbigint);

-- float
set hive.disable.unsafe.external.table.operations=false;
EXPLAIN select count(*) from alltypesorc_small join druid_table_alltypesorc on (alltypesorc_small.cfloat = druid_table_alltypesorc.cfloat);
select count(*) from alltypesorc_small join druid_table_alltypesorc on (alltypesorc_small.cfloat = druid_table_alltypesorc.cfloat);
set hive.disable.unsafe.external.table.operations=true;
select count(*) from alltypesorc_small join druid_table_alltypesorc on (alltypesorc_small.cfloat = druid_table_alltypesorc.cfloat);

-- double
set hive.disable.unsafe.external.table.operations=false;
EXPLAIN select count(*) from alltypesorc_small join druid_table_alltypesorc on (alltypesorc_small.cdouble = druid_table_alltypesorc.cdouble);
set hive.disable.unsafe.external.table.operations=true;
select count(*) from alltypesorc_small join druid_table_alltypesorc on (alltypesorc_small.cdouble = druid_table_alltypesorc.cdouble);
set hive.disable.unsafe.external.table.operations=true;

-- timestamp
set hive.disable.unsafe.external.table.operations=false;
EXPLAIN select count(*) from alltypesorc_small join druid_table_alltypesorc on (alltypesorc_small.ctimestamp1 = druid_table_alltypesorc.`__time`);
select count(*) from alltypesorc_small join druid_table_alltypesorc on (alltypesorc_small.ctimestamp1 = cast(druid_table_alltypesorc.`__time` as timestamp));
set hive.disable.unsafe.external.table.operations=true;
select count(*) from alltypesorc_small join druid_table_alltypesorc on (alltypesorc_small.ctimestamp1 = cast(druid_table_alltypesorc.`__time` as timestamp));

-- boolean
set hive.disable.unsafe.external.table.operations=false;
EXPLAIN select count(*) from alltypesorc_small join druid_table_alltypesorc on (alltypesorc_small.cboolean1 = druid_table_alltypesorc.cboolean1);
select count(*) from alltypesorc_small join druid_table_alltypesorc on (alltypesorc_small.cboolean1 = druid_table_alltypesorc.cboolean1);
set hive.disable.unsafe.external.table.operations=true;
select count(*) from alltypesorc_small join druid_table_alltypesorc on (alltypesorc_small.cboolean1 = druid_table_alltypesorc.cboolean1);


-- Test Casts

set hive.disable.unsafe.external.table.operations=false;
EXPLAIN select count(*) from alltypesorc_small join druid_table_alltypesorc on (cast(alltypesorc_small.cint as string) = druid_table_alltypesorc.cintstring);
select count(*) from alltypesorc_small join druid_table_alltypesorc on (cast(alltypesorc_small.cint as string) = druid_table_alltypesorc.cintstring);
set hive.disable.unsafe.external.table.operations=true;
select count(*) from alltypesorc_small join druid_table_alltypesorc on (cast(alltypesorc_small.cint as string) = druid_table_alltypesorc.cintstring);


set hive.disable.unsafe.external.table.operations=false;
EXPLAIN select count(*) from alltypesorc_small join druid_table_alltypesorc on (cast(alltypesorc_small.cdouble as string) = druid_table_alltypesorc.cdoublestring);
set hive.disable.unsafe.external.table.operations=true;
select count(*) from alltypesorc_small join druid_table_alltypesorc on (cast(alltypesorc_small.cdouble as string) = druid_table_alltypesorc.cdoublestring);
set hive.disable.unsafe.external.table.operations=true;
select count(*) from alltypesorc_small join druid_table_alltypesorc on (cast(alltypesorc_small.cdouble as string) = druid_table_alltypesorc.cdoublestring);


set hive.disable.unsafe.external.table.operations=false;
EXPLAIN select count(*) from alltypesorc_small join druid_table_alltypesorc on (cast(alltypesorc_small.cfloat as string) = druid_table_alltypesorc.cfloatstring);
select count(*) from alltypesorc_small join druid_table_alltypesorc on (cast(alltypesorc_small.cfloat as string) = druid_table_alltypesorc.cfloatstring);
set hive.disable.unsafe.external.table.operations=true;
select count(*) from alltypesorc_small join druid_table_alltypesorc on (cast(alltypesorc_small.cfloat as string) = druid_table_alltypesorc.cfloatstring);
