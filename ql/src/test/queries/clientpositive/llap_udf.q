--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.execution.mode=llap;
set hive.llap.execution.mode=all;
set hive.fetch.task.conversion=none;
set hive.llap.allow.permanent.fns=true;

drop table if exists src_orc_n0;
create table src_orc_n0 stored as orc as select * from src;

-- Not using GenericUDFTestGetJavaBoolean; that is already registered when tests begin

CREATE TEMPORARY FUNCTION test_udf0 AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFEvaluateNPE';

set hive.llap.execution.mode=auto;
EXPLAIN SELECT test_udf0(cast(key as string)) from src_orc_n0;

set hive.llap.execution.mode=all;
CREATE FUNCTION test_udf2 AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFTestGetJavaString';
CREATE FUNCTION test_udf3 AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFTestGetJavaString';
CREATE FUNCTION test_udf4 AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFEvaluateNPE';

EXPLAIN
SELECT test_udf2(cast(key as string)), test_udf3(cast(key as string)), test_udf4(cast(key as string)) from src_orc_n0;

set hive.llap.execution.mode=auto;
-- Verification is based on classes, so 0 would work based on 4.
EXPLAIN
SELECT test_udf0(cast(key as string)) from src_orc_n0;

DROP FUNCTION test_udf2;

set hive.llap.execution.mode=all;
-- ...verify that 3 still works
EXPLAIN
SELECT test_udf3(cast(key as string)), test_udf4(cast(key as string)) from src_orc_n0;

DROP FUNCTION test_udf4;

set hive.llap.execution.mode=auto;
-- ...now 0 should stop working
EXPLAIN
SELECT test_udf0(cast(key as string)) from src_orc_n0;

set hive.llap.allow.permanent.fns=false;

EXPLAIN
SELECT test_udf3(cast(key as string)) from src_orc_n0;


drop table if exists src_orc_n0;
set hive.execution.mode=container;
