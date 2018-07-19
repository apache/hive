-- Create a base table to be used for loading data: Begin
drop table if exists testAltCol_n1;
create table testAltCol_n1
(cId        TINYINT,
 cBigInt    BIGINT,
 cInt       INT,
 cSmallInt  SMALLINT,
 cTinyint   TINYINT);

insert into testAltCol_n1 values
(1,
 1234567890123456789,
 1234567890,
 12345,
 123);

insert into testAltCol_n1 values
(2,
 1,
 2,
 3,
 4);

insert into testAltCol_n1 values
(3,
 1234567890123456789,
 1234567890,
 12345,
 123);

insert into testAltCol_n1 values
(4,
 -1234567890123456789,
 -1234567890,
 -12345,
 -123);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltCol_n1 order by cId;
-- Create a base table to be used for loading data: End

-- Enable change of column type
SET hive.metastore.disallow.incompatible.col.type.changes=false;

-- Disable vectorization
SET hive.vectorized.execution.enabled=false;

-- Text type: Begin
drop table if exists testAltColT_n1;

create table testAltColT_n1 stored as textfile as select * from testAltCol_n1;

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColT_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to bigint
alter table testAltColT_n1 replace columns
(cId        TINYINT,
 cBigInt    BIGINT,
 cInt       BIGINT,
 cSmallInt  BIGINT,
 cTinyint   BIGINT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColT_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to int
alter table testAltColT_n1 replace columns
(cId        TINYINT,
 cBigInt    INT,
 cInt       INT,
 cSmallInt  INT,
 cTinyint   INT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColT_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to smallint
alter table testAltColT_n1 replace columns
(cId        TINYINT,
 cBigInt    SMALLINT,
 cInt       SMALLINT,
 cSmallInt  SMALLINT,
 cTinyint   SMALLINT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColT_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to tinyint
alter table testAltColT_n1 replace columns
(cId        TINYINT,
 cBigInt    TINYINT,
 cInt       TINYINT,
 cSmallInt  TINYINT,
 cTinyint   TINYINT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColT_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to float
alter table testAltColT_n1 replace columns
(cId        TINYINT,
 cBigInt    FLOAT,
 cInt       FLOAT,
 cSmallInt  FLOAT,
 cTinyint   FLOAT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColT_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to double
alter table testAltColT_n1 replace columns
(cId        TINYINT,
 cBigInt    DOUBLE,
 cInt       DOUBLE,
 cSmallInt  DOUBLE,
 cTinyint   DOUBLE);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColT_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- all values fit and should return all values
alter table testAltColT_n1 replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(22,2),
 cInt       DECIMAL(22,2),
 cSmallInt  DECIMAL(22,2),
 cTinyint   DECIMAL(22,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColT_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- some of big int doesn't fit and should return null where it didn't fit
alter table testAltColT_n1 replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(13,2),
 cInt       DECIMAL(13,2),
 cSmallInt  DECIMAL(13,2),
 cTinyint   DECIMAL(13,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColT_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- some of big int and int doesn't fit and should return null where it didn't fit
alter table testAltColT_n1 replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(8,2),
 cInt       DECIMAL(8,2),
 cSmallInt  DECIMAL(8,2),
 cTinyint   DECIMAL(8,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColT_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- some of big int, int and small int doesn't fit and should return null where it didn't fit
alter table testAltColT_n1 replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(6,2),
 cInt       DECIMAL(6,2),
 cSmallInt  DECIMAL(6,2),
 cTinyint   DECIMAL(6,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColT_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- only single digit fits and should return null where it didn't fit
alter table testAltColT_n1 replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(3,2),
 cInt       DECIMAL(3,2),
 cSmallInt  DECIMAL(3,2),
 cTinyint   DECIMAL(3,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColT_n1 order by cId;

drop table if exists testAltColT_n1;
-- Text type: End

-- Sequence File type: Begin
drop table if exists testAltColSF_n1;

create table testAltColSF_n1 stored as sequencefile as select * from testAltCol_n1;

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColSF_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to bigint
alter table testAltColSF_n1 replace columns
(cId        TINYINT,
 cBigInt    BIGINT,
 cInt       BIGINT,
 cSmallInt  BIGINT,
 cTinyint   BIGINT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColSF_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to int
alter table testAltColSF_n1 replace columns
(cId        TINYINT,
 cBigInt    INT,
 cInt       INT,
 cSmallInt  INT,
 cTinyint   INT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColSF_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to smallint
alter table testAltColSF_n1 replace columns
(cId        TINYINT,
 cBigInt    SMALLINT,
 cInt       SMALLINT,
 cSmallInt  SMALLINT,
 cTinyint   SMALLINT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColSF_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to tinyint
alter table testAltColSF_n1 replace columns
(cId        TINYINT,
 cBigInt    TINYINT,
 cInt       TINYINT,
 cSmallInt  TINYINT,
 cTinyint   TINYINT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColSF_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to float
alter table testAltColSF_n1 replace columns
(cId        TINYINT,
 cBigInt    FLOAT,
 cInt       FLOAT,
 cSmallInt  FLOAT,
 cTinyint   FLOAT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColSF_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to double
alter table testAltColSF_n1 replace columns
(cId        TINYINT,
 cBigInt    DOUBLE,
 cInt       DOUBLE,
 cSmallInt  DOUBLE,
 cTinyint   DOUBLE);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColSF_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- all values fit and should return all values
alter table testAltColSF_n1 replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(22,2),
 cInt       DECIMAL(22,2),
 cSmallInt  DECIMAL(22,2),
 cTinyint   DECIMAL(22,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColSF_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- some of big int doesn't fit and should return null where it didn't fit
alter table testAltColSF_n1 replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(13,2),
 cInt       DECIMAL(13,2),
 cSmallInt  DECIMAL(13,2),
 cTinyint   DECIMAL(13,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColSF_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- some of big int and int doesn't fit and should return null where it didn't fit
alter table testAltColSF_n1 replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(8,2),
 cInt       DECIMAL(8,2),
 cSmallInt  DECIMAL(8,2),
 cTinyint   DECIMAL(8,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColSF_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- some of big int, int and small int doesn't fit and should return null where it didn't fit
alter table testAltColSF_n1 replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(6,2),
 cInt       DECIMAL(6,2),
 cSmallInt  DECIMAL(6,2),
 cTinyint   DECIMAL(6,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColSF_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- only single digit fits and should return null where it didn't fit
alter table testAltColSF_n1 replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(3,2),
 cInt       DECIMAL(3,2),
 cSmallInt  DECIMAL(3,2),
 cTinyint   DECIMAL(3,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColSF_n1 order by cId;

drop table if exists testAltColSF_n1;
-- Sequence File type: End

-- RCFile type: Begin
drop table if exists testAltColRCF_n1;

create table testAltColRCF_n1 stored as rcfile as select * from testAltCol_n1;

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColRCF_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to bigint
alter table testAltColRCF_n1 replace columns
(cId        TINYINT,
 cBigInt    BIGINT,
 cInt       BIGINT,
 cSmallInt  BIGINT,
 cTinyint   BIGINT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColRCF_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to int
alter table testAltColRCF_n1 replace columns
(cId        TINYINT,
 cBigInt    INT,
 cInt       INT,
 cSmallInt  INT,
 cTinyint   INT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColRCF_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to smallint
alter table testAltColRCF_n1 replace columns
(cId        TINYINT,
 cBigInt    SMALLINT,
 cInt       SMALLINT,
 cSmallInt  SMALLINT,
 cTinyint   SMALLINT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColRCF_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to tinyint
alter table testAltColRCF_n1 replace columns
(cId        TINYINT,
 cBigInt    TINYINT,
 cInt       TINYINT,
 cSmallInt  TINYINT,
 cTinyint   TINYINT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColRCF_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to float
alter table testAltColRCF_n1 replace columns
(cId        TINYINT,
 cBigInt    FLOAT,
 cInt       FLOAT,
 cSmallInt  FLOAT,
 cTinyint   FLOAT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColRCF_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to double
alter table testAltColRCF_n1 replace columns
(cId        TINYINT,
 cBigInt    DOUBLE,
 cInt       DOUBLE,
 cSmallInt  DOUBLE,
 cTinyint   DOUBLE);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColRCF_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- all values fit and should return all values
alter table testAltColRCF_n1 replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(22,2),
 cInt       DECIMAL(22,2),
 cSmallInt  DECIMAL(22,2),
 cTinyint   DECIMAL(22,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColRCF_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- some of big int doesn't fit and should return null where it didn't fit
alter table testAltColRCF_n1 replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(13,2),
 cInt       DECIMAL(13,2),
 cSmallInt  DECIMAL(13,2),
 cTinyint   DECIMAL(13,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColRCF_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- some of big int and int doesn't fit and should return null where it didn't fit
alter table testAltColRCF_n1 replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(8,2),
 cInt       DECIMAL(8,2),
 cSmallInt  DECIMAL(8,2),
 cTinyint   DECIMAL(8,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColRCF_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- some of big int, int and small int doesn't fit and should return null where it didn't fit
alter table testAltColRCF_n1 replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(6,2),
 cInt       DECIMAL(6,2),
 cSmallInt  DECIMAL(6,2),
 cTinyint   DECIMAL(6,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColRCF_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- only single digit fits and should return null where it didn't fit
alter table testAltColRCF_n1 replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(3,2),
 cInt       DECIMAL(3,2),
 cSmallInt  DECIMAL(3,2),
 cTinyint   DECIMAL(3,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColRCF_n1 order by cId;

drop table if exists testAltColRCF_n1;
-- RCFile type: End

-- ORC type: Begin
drop table if exists testAltColORC_n1;

create table testAltColORC_n1 stored as orc as select * from testAltCol_n1;

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColORC_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to bigint
alter table testAltColORC_n1 replace columns
(cId        TINYINT,
 cBigInt    BIGINT,
 cInt       BIGINT,
 cSmallInt  BIGINT,
 cTinyint   BIGINT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColORC_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to int
alter table testAltColORC_n1 replace columns
(cId        TINYINT,
 cBigInt    INT,
 cInt       INT,
 cSmallInt  INT,
 cTinyint   INT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColORC_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to smallint
alter table testAltColORC_n1 replace columns
(cId        TINYINT,
 cBigInt    SMALLINT,
 cInt       SMALLINT,
 cSmallInt  SMALLINT,
 cTinyint   SMALLINT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColORC_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to tinyint
alter table testAltColORC_n1 replace columns
(cId        TINYINT,
 cBigInt    TINYINT,
 cInt       TINYINT,
 cSmallInt  TINYINT,
 cTinyint   TINYINT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColORC_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to float
alter table testAltColORC_n1 replace columns
(cId        TINYINT,
 cBigInt    FLOAT,
 cInt       FLOAT,
 cSmallInt  FLOAT,
 cTinyint   FLOAT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColORC_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to double
alter table testAltColORC_n1 replace columns
(cId        TINYINT,
 cBigInt    DOUBLE,
 cInt       DOUBLE,
 cSmallInt  DOUBLE,
 cTinyint   DOUBLE);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColORC_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- all values fit and should return all values
alter table testAltColORC_n1 replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(22,2),
 cInt       DECIMAL(22,2),
 cSmallInt  DECIMAL(22,2),
 cTinyint   DECIMAL(22,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColORC_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- some of big int doesn't fit and should return null where it didn't fit
alter table testAltColORC_n1 replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(13,2),
 cInt       DECIMAL(13,2),
 cSmallInt  DECIMAL(13,2),
 cTinyint   DECIMAL(13,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColORC_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- some of big int and int doesn't fit and should return null where it didn't fit
alter table testAltColORC_n1 replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(8,2),
 cInt       DECIMAL(8,2),
 cSmallInt  DECIMAL(8,2),
 cTinyint   DECIMAL(8,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColORC_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- some of big int, int and small int doesn't fit and should return null where it didn't fit
alter table testAltColORC_n1 replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(6,2),
 cInt       DECIMAL(6,2),
 cSmallInt  DECIMAL(6,2),
 cTinyint   DECIMAL(6,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColORC_n1 order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- only single digit fits and should return null where it didn't fit
alter table testAltColORC_n1 replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(3,2),
 cInt       DECIMAL(3,2),
 cSmallInt  DECIMAL(3,2),
 cTinyint   DECIMAL(3,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColORC_n1 order by cId;

drop table if exists testAltColORC_n1;
-- ORC type: End

-- Parquet type with Dictionary encoding enabled: Begin
drop table if exists testAltColPDE_n0;

create table testAltColPDE_n0 stored as parquet as select * from testAltCol_n1;

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDE_n0 order by cId;

-- bigint, int, smallint, and tinyint: type changed to bigint
alter table testAltColPDE_n0 replace columns
(cId        TINYINT,
 cBigInt    BIGINT,
 cInt       BIGINT,
 cSmallInt  BIGINT,
 cTinyint   BIGINT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDE_n0 order by cId;

-- bigint, int, smallint, and tinyint: type changed to int
alter table testAltColPDE_n0 replace columns
(cId        TINYINT,
 cBigInt    INT,
 cInt       INT,
 cSmallInt  INT,
 cTinyint   INT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDE_n0 order by cId;

-- bigint, int, smallint, and tinyint: type changed to smallint
alter table testAltColPDE_n0 replace columns
(cId        TINYINT,
 cBigInt    SMALLINT,
 cInt       SMALLINT,
 cSmallInt  SMALLINT,
 cTinyint   SMALLINT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDE_n0 order by cId;

-- bigint, int, smallint, and tinyint: type changed to tinyint
alter table testAltColPDE_n0 replace columns
(cId        TINYINT,
 cBigInt    TINYINT,
 cInt       TINYINT,
 cSmallInt  TINYINT,
 cTinyint   TINYINT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDE_n0 order by cId;

-- bigint, int, smallint, and tinyint: type changed to float
alter table testAltColPDE_n0 replace columns
(cId        TINYINT,
 cBigInt    FLOAT,
 cInt       FLOAT,
 cSmallInt  FLOAT,
 cTinyint   FLOAT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDE_n0 order by cId;

-- bigint, int, smallint, and tinyint: type changed to double
alter table testAltColPDE_n0 replace columns
(cId        TINYINT,
 cBigInt    DOUBLE,
 cInt       DOUBLE,
 cSmallInt  DOUBLE,
 cTinyint   DOUBLE);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDE_n0 order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- all values fit and should return all values
alter table testAltColPDE_n0 replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(22,2),
 cInt       DECIMAL(22,2),
 cSmallInt  DECIMAL(22,2),
 cTinyint   DECIMAL(22,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDE_n0 order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- some of big int doesn't fit and should return null where it didn't fit
alter table testAltColPDE_n0 replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(13,2),
 cInt       DECIMAL(13,2),
 cSmallInt  DECIMAL(13,2),
 cTinyint   DECIMAL(13,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDE_n0 order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- some of big int and int doesn't fit and should return null where it didn't fit
alter table testAltColPDE_n0 replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(8,2),
 cInt       DECIMAL(8,2),
 cSmallInt  DECIMAL(8,2),
 cTinyint   DECIMAL(8,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDE_n0 order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- some of big int, int and small int doesn't fit and should return null where it didn't fit
alter table testAltColPDE_n0 replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(6,2),
 cInt       DECIMAL(6,2),
 cSmallInt  DECIMAL(6,2),
 cTinyint   DECIMAL(6,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDE_n0 order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- only single digit fits and should return null where it didn't fit
alter table testAltColPDE_n0 replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(3,2),
 cInt       DECIMAL(3,2),
 cSmallInt  DECIMAL(3,2),
 cTinyint   DECIMAL(3,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDE_n0 order by cId;

drop table if exists testAltColPDE_n0;
-- Parquet type with Dictionary encoding enabled: End

-- Parquet type with Dictionary encoding disabled: Begin
drop table if exists testAltColPDD_n0;

create table testAltColPDD_n0 stored as parquet tblproperties ("parquet.enable.dictionary"="false") as
select * from testAltCol_n1;

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDD_n0 order by cId;

-- bigint, int, smallint, and tinyint: type changed to bigint
alter table testAltColPDD_n0 replace columns
(cId        TINYINT,
 cBigInt    BIGINT,
 cInt       BIGINT,
 cSmallInt  BIGINT,
 cTinyint   BIGINT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDD_n0 order by cId;

-- bigint, int, smallint, and tinyint: type changed to int
alter table testAltColPDD_n0 replace columns
(cId        TINYINT,
 cBigInt    INT,
 cInt       INT,
 cSmallInt  INT,
 cTinyint   INT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDD_n0 order by cId;

-- bigint, int, smallint, and tinyint: type changed to smallint
alter table testAltColPDD_n0 replace columns
(cId        TINYINT,
 cBigInt    SMALLINT,
 cInt       SMALLINT,
 cSmallInt  SMALLINT,
 cTinyint   SMALLINT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDD_n0 order by cId;

-- bigint, int, smallint, and tinyint: type changed to tinyint
alter table testAltColPDD_n0 replace columns
(cId        TINYINT,
 cBigInt    TINYINT,
 cInt       TINYINT,
 cSmallInt  TINYINT,
 cTinyint   TINYINT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDD_n0 order by cId;

-- bigint, int, smallint, and tinyint: type changed to float
alter table testAltColPDD_n0 replace columns
(cId        TINYINT,
 cBigInt    FLOAT,
 cInt       FLOAT,
 cSmallInt  FLOAT,
 cTinyint   FLOAT);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDD_n0 order by cId;

-- bigint, int, smallint, and tinyint: type changed to double
alter table testAltColPDD_n0 replace columns
(cId        TINYINT,
 cBigInt    DOUBLE,
 cInt       DOUBLE,
 cSmallInt  DOUBLE,
 cTinyint   DOUBLE);

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDD_n0 order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- all values fit and should return all values
alter table testAltColPDD_n0 replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(22,2),
 cInt       DECIMAL(22,2),
 cSmallInt  DECIMAL(22,2),
 cTinyint   DECIMAL(22,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDD_n0 order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- some of big int doesn't fit and should return null where it didn't fit
alter table testAltColPDD_n0 replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(13,2),
 cInt       DECIMAL(13,2),
 cSmallInt  DECIMAL(13,2),
 cTinyint   DECIMAL(13,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDD_n0 order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- some of big int and int doesn't fit and should return null where it didn't fit
alter table testAltColPDD_n0 replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(8,2),
 cInt       DECIMAL(8,2),
 cSmallInt  DECIMAL(8,2),
 cTinyint   DECIMAL(8,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDD_n0 order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- some of big int, int and small int doesn't fit and should return null where it didn't fit
alter table testAltColPDD_n0 replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(6,2),
 cInt       DECIMAL(6,2),
 cSmallInt  DECIMAL(6,2),
 cTinyint   DECIMAL(6,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDD_n0 order by cId;

-- bigint, int, smallint, and tinyint: type changed to decimal
-- only single digit fits and should return null where it didn't fit
alter table testAltColPDD_n0 replace columns
(cId        TINYINT,
 cBigInt    DECIMAL(3,2),
 cInt       DECIMAL(3,2),
 cSmallInt  DECIMAL(3,2),
 cTinyint   DECIMAL(3,2));

select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColPDD_n0 order by cId;

drop table if exists testAltColPDD_n0;
-- Parquet type with Dictionary encoding enabled: End
