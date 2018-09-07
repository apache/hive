-- The data stored (text, sequence file, rc file, orc, parquet with and without dictionary
-- encryption) as bigint, int, smallint and tinyint is read back as Bigint, int, smallint
-- tinyint, float, double, and decimal.  This is done after the type is changed in HMS
-- through alter table.  Vectorization is enabled for this test.

-- Create a base table to be used for loading data: Begin
drop table if exists testAltCol;
create table testAltCol
(cId        TINYINT,
 cBigInt    BIGINT,
 cInt       INT,
 cSmallInt  SMALLINT,
 cTinyint   TINYINT);

insert into testAltCol values
(1,
 1234567890123456789,
 1234567890,
 12345,
 123);

insert into testAltCol values
(2,
 1,
 2,
 3,
 4);

insert into testAltCol values
(3,
 1234567890123456789,
 1234567890,
 12345,
 123);

insert into testAltCol values
(4,
 -1234567890123456789,
 -1234567890,
 -12345,
 -123);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltCol order by cId;
-- Create a base table to be used for loading data: End

-- Enable change of column type
SET hive.metastore.disallow.incompatible.col.type.changes=false;

-- Enable vectorization
SET hive.vectorized.execution.enabled=true;

-- Text type: Begin
drop table if exists testAltColT;

create table testAltColT stored as textfile as select * from testAltCol;

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColT order by cId;

-- bigint, int, smallint, and tinyint: type changed to bigint
alter table testAltColT replace columns
(cId        TINYINT,
 cBigInt    BIGINT,
 cInt       BIGINT,
 cSmallInt  BIGINT,
 cTinyint   BIGINT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColT order by cId;

-- bigint, int, smallint, and tinyint: type changed to int
alter table testAltColT replace columns
(cId        TINYINT,
 cBigInt    INT,
 cInt       INT,
 cSmallInt  INT,
 cTinyint   INT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColT order by cId;

-- bigint, int, smallint, and tinyint: type changed to smallint
alter table testAltColT replace columns
(cId        TINYINT,
 cBigInt    SMALLINT,
 cInt       SMALLINT,
 cSmallInt  SMALLINT,
 cTinyint   SMALLINT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColT order by cId;

-- bigint, int, smallint, and tinyint: type changed to tinyint
alter table testAltColT replace columns
(cId        TINYINT,
 cBigInt    TINYINT,
 cInt       TINYINT,
 cSmallInt  TINYINT,
 cTinyint   TINYINT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColT order by cId;

-- bigint, int, smallint, and tinyint: type changed to float
alter table testAltColT replace columns
(cId        TINYINT,
 cBigInt    FLOAT,
 cInt       FLOAT,
 cSmallInt  FLOAT,
 cTinyint   FLOAT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColT order by cId;

-- bigint, int, smallint, and tinyint: type changed to double
alter table testAltColT replace columns
(cId        TINYINT,
 cBigInt    DOUBLE,
 cInt       DOUBLE,
 cSmallInt  DOUBLE,
 cTinyint   DOUBLE);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColT order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- all values fit and should return all values
alter table testAltColT replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(22,2),
 cInt       DECIMAL(22,2),
 cSmallInt  DECIMAL(22,2),
 cTinyint   DECIMAL(22,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColT order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- some of big int doesn't fit and should return null where it didn't fit
alter table testAltColT replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(13,2),
 cInt       DECIMAL(13,2),
 cSmallInt  DECIMAL(13,2),
 cTinyint   DECIMAL(13,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColT order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- some of big int and int doesn't fit and should return null where it didn't fit
alter table testAltColT replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(8,2),
 cInt       DECIMAL(8,2),
 cSmallInt  DECIMAL(8,2),
 cTinyint   DECIMAL(8,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColT order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- some of big int, int and small int doesn't fit and should return null where it didn't fit
alter table testAltColT replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(6,2),
 cInt       DECIMAL(6,2),
 cSmallInt  DECIMAL(6,2),
 cTinyint   DECIMAL(6,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColT order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- only single digit fits and should return null where it didn't fit
alter table testAltColT replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(3,2),
 cInt       DECIMAL(3,2),
 cSmallInt  DECIMAL(3,2),
 cTinyint   DECIMAL(3,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColT order by cId;

drop table if exists testAltColT;
-- Text type: End

-- Sequence File type: Begin
drop table if exists testAltColSF;

create table testAltColSF stored as sequencefile as select * from testAltCol;

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColSF order by cId;

-- bigint, int, smallint, and tinyint: type changed to bigint
alter table testAltColSF replace columns
(cId        TINYINT,
 cBigInt    BIGINT,
 cInt       BIGINT,
 cSmallInt  BIGINT,
 cTinyint   BIGINT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColSF order by cId;

-- bigint, int, smallint, and tinyint: type changed to int
alter table testAltColSF replace columns
(cId        TINYINT,
 cBigInt    INT,
 cInt       INT,
 cSmallInt  INT,
 cTinyint   INT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColSF order by cId;

-- bigint, int, smallint, and tinyint: type changed to smallint
alter table testAltColSF replace columns
(cId        TINYINT,
 cBigInt    SMALLINT,
 cInt       SMALLINT,
 cSmallInt  SMALLINT,
 cTinyint   SMALLINT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColSF order by cId;

-- bigint, int, smallint, and tinyint: type changed to tinyint
alter table testAltColSF replace columns
(cId        TINYINT,
 cBigInt    TINYINT,
 cInt       TINYINT,
 cSmallInt  TINYINT,
 cTinyint   TINYINT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColSF order by cId;

-- bigint, int, smallint, and tinyint: type changed to float
alter table testAltColSF replace columns
(cId        TINYINT,
 cBigInt    FLOAT,
 cInt       FLOAT,
 cSmallInt  FLOAT,
 cTinyint   FLOAT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColSF order by cId;

-- bigint, int, smallint, and tinyint: type changed to double
alter table testAltColSF replace columns
(cId        TINYINT,
 cBigInt    DOUBLE,
 cInt       DOUBLE,
 cSmallInt  DOUBLE,
 cTinyint   DOUBLE);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColSF order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- all values fit and should return all values
alter table testAltColSF replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(22,2),
 cInt       DECIMAL(22,2),
 cSmallInt  DECIMAL(22,2),
 cTinyint   DECIMAL(22,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColSF order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- some of big int doesn't fit and should return null where it didn't fit
alter table testAltColSF replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(13,2),
 cInt       DECIMAL(13,2),
 cSmallInt  DECIMAL(13,2),
 cTinyint   DECIMAL(13,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColSF order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- some of big int and int doesn't fit and should return null where it didn't fit
alter table testAltColSF replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(8,2),
 cInt       DECIMAL(8,2),
 cSmallInt  DECIMAL(8,2),
 cTinyint   DECIMAL(8,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColSF order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- some of big int, int and small int doesn't fit and should return null where it didn't fit
alter table testAltColSF replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(6,2),
 cInt       DECIMAL(6,2),
 cSmallInt  DECIMAL(6,2),
 cTinyint   DECIMAL(6,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColSF order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- only single digit fits and should return null where it didn't fit
alter table testAltColSF replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(3,2),
 cInt       DECIMAL(3,2),
 cSmallInt  DECIMAL(3,2),
 cTinyint   DECIMAL(3,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColSF order by cId;

drop table if exists testAltColSF;
-- Sequence File type: End

-- RCFile type: Begin
drop table if exists testAltColRCF;

create table testAltColRCF stored as rcfile as select * from testAltCol;

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColRCF order by cId;

-- bigint, int, smallint, and tinyint: type changed to bigint
alter table testAltColRCF replace columns
(cId        TINYINT,
 cBigInt    BIGINT,
 cInt       BIGINT,
 cSmallInt  BIGINT,
 cTinyint   BIGINT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColRCF order by cId;

-- bigint, int, smallint, and tinyint: type changed to int
alter table testAltColRCF replace columns
(cId        TINYINT,
 cBigInt    INT,
 cInt       INT,
 cSmallInt  INT,
 cTinyint   INT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColRCF order by cId;

-- bigint, int, smallint, and tinyint: type changed to smallint
alter table testAltColRCF replace columns
(cId        TINYINT,
 cBigInt    SMALLINT,
 cInt       SMALLINT,
 cSmallInt  SMALLINT,
 cTinyint   SMALLINT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColRCF order by cId;

-- bigint, int, smallint, and tinyint: type changed to tinyint
alter table testAltColRCF replace columns
(cId        TINYINT,
 cBigInt    TINYINT,
 cInt       TINYINT,
 cSmallInt  TINYINT,
 cTinyint   TINYINT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColRCF order by cId;

-- bigint, int, smallint, and tinyint: type changed to float
alter table testAltColRCF replace columns
(cId        TINYINT,
 cBigInt    FLOAT,
 cInt       FLOAT,
 cSmallInt  FLOAT,
 cTinyint   FLOAT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColRCF order by cId;

-- bigint, int, smallint, and tinyint: type changed to double
alter table testAltColRCF replace columns
(cId        TINYINT,
 cBigInt    DOUBLE,
 cInt       DOUBLE,
 cSmallInt  DOUBLE,
 cTinyint   DOUBLE);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColRCF order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- all values fit and should return all values
alter table testAltColRCF replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(22,2),
 cInt       DECIMAL(22,2),
 cSmallInt  DECIMAL(22,2),
 cTinyint   DECIMAL(22,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColRCF order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- some of big int doesn't fit and should return null where it didn't fit
alter table testAltColRCF replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(13,2),
 cInt       DECIMAL(13,2),
 cSmallInt  DECIMAL(13,2),
 cTinyint   DECIMAL(13,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColRCF order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- some of big int and int doesn't fit and should return null where it didn't fit
alter table testAltColRCF replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(8,2),
 cInt       DECIMAL(8,2),
 cSmallInt  DECIMAL(8,2),
 cTinyint   DECIMAL(8,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColRCF order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- some of big int, int and small int doesn't fit and should return null where it didn't fit
alter table testAltColRCF replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(6,2),
 cInt       DECIMAL(6,2),
 cSmallInt  DECIMAL(6,2),
 cTinyint   DECIMAL(6,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColRCF order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- only single digit fits and should return null where it didn't fit
alter table testAltColRCF replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(3,2),
 cInt       DECIMAL(3,2),
 cSmallInt  DECIMAL(3,2),
 cTinyint   DECIMAL(3,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColRCF order by cId;

drop table if exists testAltColRCF;
-- RCFile type: End

-- ORC type: Begin
drop table if exists testAltColORC;

create table testAltColORC stored as orc as select * from testAltCol;

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColORC order by cId;

-- bigint, int, smallint, and tinyint: type changed to bigint
alter table testAltColORC replace columns
(cId        TINYINT,
 cBigInt    BIGINT,
 cInt       BIGINT,
 cSmallInt  BIGINT,
 cTinyint   BIGINT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColORC order by cId;

-- bigint, int, smallint, and tinyint: type changed to int
alter table testAltColORC replace columns
(cId        TINYINT,
 cBigInt    INT,
 cInt       INT,
 cSmallInt  INT,
 cTinyint   INT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColORC order by cId;

-- bigint, int, smallint, and tinyint: type changed to smallint
alter table testAltColORC replace columns
(cId        TINYINT,
 cBigInt    SMALLINT,
 cInt       SMALLINT,
 cSmallInt  SMALLINT,
 cTinyint   SMALLINT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColORC order by cId;

-- bigint, int, smallint, and tinyint: type changed to tinyint
alter table testAltColORC replace columns
(cId        TINYINT,
 cBigInt    TINYINT,
 cInt       TINYINT,
 cSmallInt  TINYINT,
 cTinyint   TINYINT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColORC order by cId;

-- bigint, int, smallint, and tinyint: type changed to float
alter table testAltColORC replace columns
(cId        TINYINT,
 cBigInt    FLOAT,
 cInt       FLOAT,
 cSmallInt  FLOAT,
 cTinyint   FLOAT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColORC order by cId;

-- bigint, int, smallint, and tinyint: type changed to double
alter table testAltColORC replace columns
(cId        TINYINT,
 cBigInt    DOUBLE,
 cInt       DOUBLE,
 cSmallInt  DOUBLE,
 cTinyint   DOUBLE);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColORC order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- all values fit and should return all values
alter table testAltColORC replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(22,2),
 cInt       DECIMAL(22,2),
 cSmallInt  DECIMAL(22,2),
 cTinyint   DECIMAL(22,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColORC order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- some of big int doesn't fit and should return null where it didn't fit
alter table testAltColORC replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(13,2),
 cInt       DECIMAL(13,2),
 cSmallInt  DECIMAL(13,2),
 cTinyint   DECIMAL(13,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColORC order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- some of big int and int doesn't fit and should return null where it didn't fit
alter table testAltColORC replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(8,2),
 cInt       DECIMAL(8,2),
 cSmallInt  DECIMAL(8,2),
 cTinyint   DECIMAL(8,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColORC order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- some of big int, int and small int doesn't fit and should return null where it didn't fit
alter table testAltColORC replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(6,2),
 cInt       DECIMAL(6,2),
 cSmallInt  DECIMAL(6,2),
 cTinyint   DECIMAL(6,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColORC order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- only single digit fits and should return null where it didn't fit
alter table testAltColORC replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(3,2),
 cInt       DECIMAL(3,2),
 cSmallInt  DECIMAL(3,2),
 cTinyint   DECIMAL(3,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColORC order by cId;

drop table if exists testAltColORC;
-- ORC type: End

-- Parquet type with Dictionary encoding enabled: Begin
drop table if exists testAltColPDE;

create table testAltColPDE stored as parquet as select * from testAltCol;

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDE order by cId;

-- bigint, int, smallint, and tinyint: type changed to bigint
alter table testAltColPDE replace columns
(cId        TINYINT,
 cBigInt    BIGINT,
 cInt       BIGINT,
 cSmallInt  BIGINT,
 cTinyint   BIGINT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDE order by cId;

-- bigint, int, smallint, and tinyint: type changed to int
alter table testAltColPDE replace columns
(cId        TINYINT,
 cBigInt    INT,
 cInt       INT,
 cSmallInt  INT,
 cTinyint   INT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDE order by cId;

-- bigint, int, smallint, and tinyint: type changed to smallint
alter table testAltColPDE replace columns
(cId        TINYINT,
 cBigInt    SMALLINT,
 cSmallInt  SMALLINT,
 cInt       SMALLINT,
 cTinyint   SMALLINT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDE order by cId;

-- bigint, int, smallint, and tinyint: type changed to tinyint
alter table testAltColPDE replace columns
(cId        TINYINT,
 cBigInt    TINYINT,
 cInt       TINYINT,
 cSmallInt  TINYINT,
 cTinyint   TINYINT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDE order by cId;

-- bigint, int, smallint, and tinyint: type changed to float
alter table testAltColPDE replace columns
(cId        TINYINT,
 cBigInt    FLOAT,
 cInt       FLOAT,
 cSmallInt  FLOAT,
 cTinyint   FLOAT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDE order by cId;

-- bigint, int, smallint, and tinyint: type changed to double
alter table testAltColPDE replace columns
(cId        TINYINT,
 cBigInt    DOUBLE,
 cInt       DOUBLE,
 cSmallInt  DOUBLE,
 cTinyint   DOUBLE);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDE order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- all values fit and should return all values
alter table testAltColPDE replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(22,2),
 cInt       DECIMAL(22,2),
 cSmallInt  DECIMAL(22,2),
 cTinyint   DECIMAL(22,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDE order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- some of big int doesn't fit and should return null where it didn't fit
alter table testAltColPDE replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(13,2),
 cInt       DECIMAL(13,2),
 cSmallInt  DECIMAL(13,2),
 cTinyint   DECIMAL(13,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDE order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- some of big int and int doesn't fit and should return null where it didn't fit
alter table testAltColPDE replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(8,2),
 cInt       DECIMAL(8,2),
 cSmallInt  DECIMAL(8,2),
 cTinyint   DECIMAL(8,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDE order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- some of big int, int and small int doesn't fit and should return null where it didn't fit
alter table testAltColPDE replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(6,2),
 cInt       DECIMAL(6,2),
 cSmallInt  DECIMAL(6,2),
 cTinyint   DECIMAL(6,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDE order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- only single digit fits and should return null where it didn't fit
alter table testAltColPDE replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(3,2),
 cInt       DECIMAL(3,2),
 cSmallInt  DECIMAL(3,2),
 cTinyint   DECIMAL(3,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDE order by cId;

drop table if exists testAltColPDE;
-- Parquet type with Dictionary encoding enabled: End

-- Parquet type with Dictionary encoding disabled: Begin
drop table if exists testAltColPDD;

create table testAltColPDD stored as parquet tblproperties ("parquet.enable.dictionary"="false") as
select * from testAltCol;

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDD order by cId;

-- bigint, int, smallint, and tinyint: type changed to bigint
alter table testAltColPDD replace columns
(cId        TINYINT,
 cBigInt    BIGINT,
 cInt       BIGINT,
 cSmallInt  BIGINT,
 cTinyint   BIGINT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDD order by cId;

-- bigint, int, smallint, and tinyint: type changed to int
alter table testAltColPDD replace columns
(cId        TINYINT,
 cBigInt    INT,
 cInt       INT,
 cSmallInt  INT,
 cTinyint   INT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDD order by cId;

-- bigint, int, smallint, and tinyint: type changed to smallint
alter table testAltColPDD replace columns
(cId        TINYINT,
 cBigInt    SMALLINT,
 cInt       SMALLINT,
 cSmallInt  SMALLINT,
 cTinyint   SMALLINT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDD order by cId;

-- bigint, int, smallint, and tinyint: type changed to tinyint
alter table testAltColPDD replace columns
(cId        TINYINT,
 cBigInt    TINYINT,
 cInt       TINYINT,
 cSmallInt  TINYINT,
 cTinyint   TINYINT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDD order by cId;

-- bigint, int, smallint, and tinyint: type changed to float
alter table testAltColPDD replace columns
(cId        TINYINT,
 cBigInt    FLOAT,
 cInt       FLOAT,
 cSmallInt  FLOAT,
 cTinyint   FLOAT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDD order by cId;

-- bigint, int, smallint, and tinyint: type changed to double
alter table testAltColPDD replace columns
(cId        TINYINT,
 cBigInt    DOUBLE,
 cInt       DOUBLE,
 cSmallInt  DOUBLE,
 cTinyint   DOUBLE);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDD order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- all values fit and should return all values
alter table testAltColPDD replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(22,2),
 cInt       DECIMAL(22,2),
 cSmallInt  DECIMAL(22,2),
 cTinyint   DECIMAL(22,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDD order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- some of big int doesn't fit and should return null where it didn't fit
alter table testAltColPDD replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(13,2),
 cInt       DECIMAL(13,2),
 cSmallInt  DECIMAL(13,2),
 cTinyint   DECIMAL(13,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDD order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- some of big int and int doesn't fit and should return null where it didn't fit
alter table testAltColPDD replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(8,2),
 cInt       DECIMAL(8,2),
 cSmallInt  DECIMAL(8,2),
 cTinyint   DECIMAL(8,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDD order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- some of big int, int and small int doesn't fit and should return null where it didn't fit
alter table testAltColPDD replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(6,2),
 cInt       DECIMAL(6,2),
 cSmallInt  DECIMAL(6,2),
 cTinyint   DECIMAL(6,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDD order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- only single digit fits and should return null where it didn't fit
alter table testAltColPDD replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(3,2),
 cInt       DECIMAL(3,2),
 cSmallInt  DECIMAL(3,2),
 cTinyint   DECIMAL(3,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDD order by cId;

drop table if exists testAltColPDD;
-- Parquet type with Dictionary encoding enabled: End

