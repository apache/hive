--! qt:dataset:alltypesorc
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

EXPLAIN VECTORIZATION DETAIL SELECT cdouble, cint, cboolean1, ctimestamp1, CAST(cdouble AS DECIMAL(20,10)), CAST(cint AS DECIMAL(23,14)), CAST(cboolean1 AS DECIMAL(5,2)), CAST(ctimestamp1 AS DECIMAL(15,0)) FROM alltypesorc WHERE cdouble IS NOT NULL AND cint IS NOT NULL AND cboolean1 IS NOT NULL AND ctimestamp1 IS NOT NULL LIMIT 10;

SELECT cdouble, cint, cboolean1, ctimestamp1, CAST(cdouble AS DECIMAL(20,10)), CAST(cint AS DECIMAL(23,14)), CAST(cboolean1 AS DECIMAL(5,2)), CAST(ctimestamp1 AS DECIMAL(15,0)) FROM alltypesorc WHERE cdouble IS NOT NULL AND cint IS NOT NULL AND cboolean1 IS NOT NULL AND ctimestamp1 IS NOT NULL LIMIT 10;

-- DECIMAL_64

CREATE TABLE alltypes_small STORED AS TEXTFILE AS SELECT * FROM alltypesorc;

EXPLAIN VECTORIZATION DETAIL
SELECT cdouble, cint, cboolean1, ctimestamp1, CAST(cdouble AS DECIMAL(20,10)), CAST(cint AS DECIMAL(23,14)), CAST(cboolean1 AS DECIMAL(5,2)), CAST(ctimestamp1 AS DECIMAL(15,0)) FROM alltypes_small WHERE cdouble IS NOT NULL AND cint IS NOT NULL AND cboolean1 IS NOT NULL AND ctimestamp1 IS NOT NULL LIMIT 10;

SELECT cdouble, cint, cboolean1, ctimestamp1, CAST(cdouble AS DECIMAL(20,10)), CAST(cint AS DECIMAL(23,14)), CAST(cboolean1 AS DECIMAL(5,2)), CAST(ctimestamp1 AS DECIMAL(15,0)) FROM alltypes_small WHERE cdouble IS NOT NULL AND cint IS NOT NULL AND cboolean1 IS NOT NULL AND ctimestamp1 IS NOT NULL LIMIT 10;

