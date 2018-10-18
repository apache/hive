
-- Create a base table to be used for loading data: Begin
drop table if exists testAltCol_n0;
create table testAltCol_n0
(cId        TINYINT,
 cTimeStamp TIMESTAMP,
 cDecimal   DECIMAL(38,18),
 cDouble    DOUBLE,
 cFloat     FLOAT,
 cBigInt    BIGINT,
 cInt       INT,
 cSmallInt  SMALLINT,
 cTinyint   TINYINT,
 cBoolean   BOOLEAN);

insert into testAltCol_n0 values
(1,
 '2017-11-07 09:02:49.999999999',
 12345678901234567890.123456789012345678,
 1.79e308,
 3.4e38,
 1234567890123456789,
 1234567890,
 12345,
 123,
 TRUE);

insert into testAltCol_n0 values
(2,
 '1400-01-01 01:01:01.000000001',
 1.1,
 2.2,
 3.3,
 1,
 2,
 3,
 4,
 FALSE);

insert into testAltCol_n0 values
(3,
 '1400-01-01 01:01:01.000000001',
 10.1,
 20.2,
 30.3,
 1234567890123456789,
 1234567890,
 12345,
 123,
 TRUE);

insert into testAltCol_n0 values
(4,
 '1400-01-01 01:01:01.000000001',
 -10.1,
 -20.2,
 -30.3,
 -1234567890123456789,
 -1234567890,
 -12345,
 -123,
 FALSE);

select cId, cTimeStamp from testAltCol_n0 order by cId;
select cId, cDecimal, cDouble, cFloat from testAltCol_n0 order by cId;
select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltCol_n0 order by cId;
select cId, cBoolean from testAltCol_n0 order by cId;
-- Create a base table to be used for loading data: Begin

-- Enable change of column type
SET hive.metastore.disallow.incompatible.col.type.changes=false;

-- Text type: Begin
-- timestamp, decimal, double, float, bigint, int, smallint, tinyint and boolean: after type
-- changed to string, varchar and char return correct data.
drop table if exists testAltColT_n0;

create table testAltColT_n0 stored as textfile as select * from testAltCol_n0;

select cId, cTimeStamp from testAltColT_n0 order by cId;
select cId, cDecimal, cDouble, cFloat from testAltColT_n0 order by cId;
select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColT_n0 order by cId;
select cId, cBoolean from testAltColT_n0 order by cId;

alter table testAltColT_n0 replace columns
(cId        TINYINT,
 cTimeStamp STRING,
 cDecimal   STRING,
 cDouble    STRING,
 cFloat     STRING,
 cBigInt    STRING,
 cInt       STRING,
 cSmallInt  STRING,
 cTinyint   STRING,
 cBoolean   STRING);

select cId, cTimeStamp from testAltColT_n0 order by cId;
select cId, cDecimal, cDouble, cFloat from testAltColT_n0 order by cId;
select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColT_n0 order by cId;
select cId, cBoolean from testAltColT_n0 order by cId;

alter table testAltColT_n0 replace columns
(cId        TINYINT,
 cTimeStamp VARCHAR(100),
 cDecimal   VARCHAR(100),
 cDouble    VARCHAR(100),
 cFloat     VARCHAR(100),
 cBigInt    VARCHAR(100),
 cInt       VARCHAR(100),
 cSmallInt  VARCHAR(100),
 cTinyint   VARCHAR(100),
 cBoolean   VARCHAR(100));

select cId, cTimeStamp from testAltColT_n0 order by cId;
select cId, cDecimal, cDouble, cFloat from testAltColT_n0 order by cId;
select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColT_n0 order by cId;
select cId, cBoolean from testAltColT_n0 order by cId;

alter table testAltColT_n0 replace columns
(cId        TINYINT,
 cTimeStamp CHAR(100),
 cDecimal   CHAR(100),
 cDouble    CHAR(100),
 cFloat     CHAR(100),
 cBigInt    CHAR(100),
 cInt       CHAR(100),
 cSmallInt  CHAR(100),
 cTinyint   CHAR(100),
 cBoolean   CHAR(100));

select cId, cTimeStamp from testAltColT_n0 order by cId;
select cId, cDecimal, cDouble, cFloat from testAltColT_n0 order by cId;
select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColT_n0 order by cId;
select cId, cBoolean from testAltColT_n0 order by cId;

alter table testAltColT_n0 replace columns
(cId        TINYINT,
 cTimeStamp VARCHAR(4),
 cDecimal   VARCHAR(4),
 cDouble    VARCHAR(4),
 cFloat     VARCHAR(4),
 cBigInt    VARCHAR(4),
 cInt       VARCHAR(4),
 cSmallInt  VARCHAR(4),
 cTinyint   VARCHAR(4),
 cBoolean   VARCHAR(4));

select cId, cTimeStamp from testAltColT_n0 order by cId;
select cId, cDecimal, cDouble, cFloat from testAltColT_n0 order by cId;
select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColT_n0 order by cId;
select cId, cBoolean from testAltColT_n0 order by cId;

alter table testAltColT_n0 replace columns
(cId        TINYINT,
 cTimeStamp CHAR(4),
 cDecimal   CHAR(4),
 cDouble    CHAR(4),
 cFloat     CHAR(4),
 cBigInt    CHAR(4),
 cInt       CHAR(4),
 cSmallInt  CHAR(4),
 cTinyint   CHAR(4),
 cBoolean   CHAR(4));

select cId, cTimeStamp from testAltColT_n0 order by cId;
select cId, cDecimal, cDouble, cFloat from testAltColT_n0 order by cId;
select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColT_n0 order by cId;
select cId, cBoolean from testAltColT_n0 order by cId;
drop table if exists testAltColT_n0;
-- Text type: End

-- Sequence File type: Begin
-- timestamp, decimal, double, float, bigint, int, smallint, tinyint and boolean: after type
-- changed to string, varchar and char return correct data.
drop table if exists testAltColSF_n0;

create table testAltColSF_n0 stored as sequencefile as select * from testAltCol_n0;

select cId, cTimeStamp from testAltColSF_n0 order by cId;
select cId, cDecimal, cDouble, cFloat from testAltColSF_n0 order by cId;
select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColSF_n0 order by cId;
select cId, cBoolean from testAltColSF_n0 order by cId;

alter table testAltColSF_n0 replace columns
(cId        TINYINT,
 cTimeStamp STRING,
 cDecimal   STRING,
 cDouble    STRING,
 cFloat     STRING,
 cBigInt    STRING,
 cInt       STRING,
 cSmallInt  STRING,
 cTinyint   STRING,
 cBoolean   STRING);

select cId, cTimeStamp from testAltColSF_n0 order by cId;
select cId, cDecimal, cDouble, cFloat from testAltColSF_n0 order by cId;
select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColSF_n0 order by cId;
select cId, cBoolean from testAltColSF_n0 order by cId;

alter table testAltColSF_n0 replace columns
(cId        TINYINT,
 cTimeStamp VARCHAR(100),
 cDecimal   VARCHAR(100),
 cDouble    VARCHAR(100),
 cFloat     VARCHAR(100),
 cBigInt    VARCHAR(100),
 cInt       VARCHAR(100),
 cSmallInt  VARCHAR(100),
 cTinyint   VARCHAR(100),
 cBoolean   VARCHAR(100));

select cId, cTimeStamp from testAltColSF_n0 order by cId;
select cId, cDecimal, cDouble, cFloat from testAltColSF_n0 order by cId;
select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColSF_n0 order by cId;
select cId, cBoolean from testAltColSF_n0 order by cId;

alter table testAltColSF_n0 replace columns
(cId        TINYINT,
 cTimeStamp CHAR(100),
 cDecimal   CHAR(100),
 cDouble    CHAR(100),
 cFloat     CHAR(100),
 cBigInt    CHAR(100),
 cInt       CHAR(100),
 cSmallInt  CHAR(100),
 cTinyint   CHAR(100),
 cBoolean   CHAR(100));

select cId, cTimeStamp from testAltColSF_n0 order by cId;
select cId, cDecimal, cDouble, cFloat from testAltColSF_n0 order by cId;
select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColSF_n0 order by cId;
select cId, cBoolean from testAltColSF_n0 order by cId;

alter table testAltColSF_n0 replace columns
(cId        TINYINT,
 cTimeStamp VARCHAR(4),
 cDecimal   VARCHAR(4),
 cDouble    VARCHAR(4),
 cFloat     VARCHAR(4),
 cBigInt    VARCHAR(4),
 cInt       VARCHAR(4),
 cSmallInt  VARCHAR(4),
 cTinyint   VARCHAR(4),
 cBoolean   VARCHAR(4));

select cId, cTimeStamp from testAltColSF_n0 order by cId;
select cId, cDecimal, cDouble, cFloat from testAltColSF_n0 order by cId;
select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColSF_n0 order by cId;
select cId, cBoolean from testAltColSF_n0 order by cId;

alter table testAltColSF_n0 replace columns
(cId        TINYINT,
 cTimeStamp CHAR(4),
 cDecimal   CHAR(4),
 cDouble    CHAR(4),
 cFloat     CHAR(4),
 cBigInt    CHAR(4),
 cInt       CHAR(4),
 cSmallInt  CHAR(4),
 cTinyint   CHAR(4),
 cBoolean   CHAR(4));

select cId, cTimeStamp from testAltColSF_n0 order by cId;
select cId, cDecimal, cDouble, cFloat from testAltColSF_n0 order by cId;
select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColSF_n0 order by cId;
select cId, cBoolean from testAltColSF_n0 order by cId;

drop table if exists testAltColSF_n0;
-- Sequence File type: End

-- ORC type: Begin
-- timestamp, decimal, double, float, bigint, int, smallint, tinyint and boolean: after type
-- changed to string, varchar and char return correct data.
drop table if exists testAltColORC_n0;

create table testAltColORC_n0 stored as orc as select * from testAltCol_n0;

select cId, cTimeStamp from testAltColORC_n0 order by cId;
select cId, cDecimal, cDouble, cFloat from testAltColORC_n0 order by cId;
select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColORC_n0 order by cId;
select cId, cBoolean from testAltColORC_n0 order by cId;

alter table testAltColORC_n0 replace columns
(cId        TINYINT,
 cTimeStamp STRING,
 cDecimal   STRING,
 cDouble    STRING,
 cFloat     STRING,
 cBigInt    STRING,
 cInt       STRING,
 cSmallInt  STRING,
 cTinyint   STRING,
 cBoolean   STRING);

select cId, cTimeStamp from testAltColORC_n0 order by cId;
select cId, cDecimal, cDouble, cFloat from testAltColORC_n0 order by cId;
select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColORC_n0 order by cId;
select cId, cBoolean from testAltColORC_n0 order by cId;

alter table testAltColORC_n0 replace columns
(cId        TINYINT,
 cTimeStamp VARCHAR(100),
 cDecimal   VARCHAR(100),
 cDouble    VARCHAR(100),
 cFloat     VARCHAR(100),
 cBigInt    VARCHAR(100),
 cInt       VARCHAR(100),
 cSmallInt  VARCHAR(100),
 cTinyint   VARCHAR(100),
 cBoolean   VARCHAR(100));

select cId, cTimeStamp from testAltColORC_n0 order by cId;
select cId, cDecimal, cDouble, cFloat from testAltColORC_n0 order by cId;
select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColORC_n0 order by cId;
select cId, cBoolean from testAltColORC_n0 order by cId;

alter table testAltColORC_n0 replace columns
(cId        TINYINT,
 cTimeStamp CHAR(100),
 cDecimal   CHAR(100),
 cDouble    CHAR(100),
 cFloat     CHAR(100),
 cBigInt    CHAR(100),
 cInt       CHAR(100),
 cSmallInt  CHAR(100),
 cTinyint   CHAR(100),
 cBoolean   CHAR(100));

select cId, cTimeStamp from testAltColORC_n0 order by cId;
select cId, cDecimal, cDouble, cFloat from testAltColORC_n0 order by cId;
select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColORC_n0 order by cId;
select cId, cBoolean from testAltColORC_n0 order by cId;

alter table testAltColORC_n0 replace columns
(cId        TINYINT,
 cTimeStamp VARCHAR(4),
 cDecimal   VARCHAR(4),
 cDouble    VARCHAR(4),
 cFloat     VARCHAR(4),
 cBigInt    VARCHAR(4),
 cInt       VARCHAR(4),
 cSmallInt  VARCHAR(4),
 cTinyint   VARCHAR(4),
 cBoolean   VARCHAR(4));

select cId, cTimeStamp from testAltColORC_n0 order by cId;
select cId, cDecimal, cDouble, cFloat from testAltColORC_n0 order by cId;
select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColORC_n0 order by cId;
select cId, cBoolean from testAltColORC_n0 order by cId;

alter table testAltColORC_n0 replace columns
(cId        TINYINT,
 cTimeStamp CHAR(4),
 cDecimal   CHAR(4),
 cDouble    CHAR(4),
 cFloat     CHAR(4),
 cBigInt    CHAR(4),
 cInt       CHAR(4),
 cSmallInt  CHAR(4),
 cTinyint   CHAR(4),
 cBoolean   CHAR(4));

select cId, cTimeStamp from testAltColORC_n0 order by cId;
select cId, cDecimal, cDouble, cFloat from testAltColORC_n0 order by cId;
select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColORC_n0 order by cId;
select cId, cBoolean from testAltColORC_n0 order by cId;

drop table if exists testAltColORC_n0;
-- ORC type: End

-- RCFile type: Begin
-- timestamp, decimal, double, float, bigint, int, smallint, tinyint and boolean: after type
-- changed to string, varchar and char return correct data.
drop table if exists testAltColRCF_n0;

create table testAltColRCF_n0 stored as rcfile as select * from testAltCol_n0;

select cId, cTimeStamp from testAltColRCF_n0 order by cId;
select cId, cDecimal, cDouble, cFloat from testAltColRCF_n0 order by cId;
select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColRCF_n0 order by cId;
select cId, cBoolean from testAltColRCF_n0 order by cId;

alter table testAltColRCF_n0 replace columns
(cId        TINYINT,
 cTimeStamp STRING,
 cDecimal   STRING,
 cDouble    STRING,
 cFloat     STRING,
 cBigInt    STRING,
 cInt       STRING,
 cSmallInt  STRING,
 cTinyint   STRING,
 cBoolean   STRING);

select cId, cTimeStamp from testAltColRCF_n0 order by cId;
select cId, cDecimal, cDouble, cFloat from testAltColRCF_n0 order by cId;
select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColRCF_n0 order by cId;
select cId, cBoolean from testAltColRCF_n0 order by cId;

alter table testAltColRCF_n0 replace columns
(cId        TINYINT,
 cTimeStamp VARCHAR(100),
 cDecimal   VARCHAR(100),
 cDouble    VARCHAR(100),
 cFloat     VARCHAR(100),
 cBigInt    VARCHAR(100),
 cInt       VARCHAR(100),
 cSmallInt  VARCHAR(100),
 cTinyint   VARCHAR(100),
 cBoolean   VARCHAR(100));

select cId, cTimeStamp from testAltColRCF_n0 order by cId;
select cId, cDecimal, cDouble, cFloat from testAltColRCF_n0 order by cId;
select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColRCF_n0 order by cId;
select cId, cBoolean from testAltColRCF_n0 order by cId;

alter table testAltColRCF_n0 replace columns
(cId        TINYINT,
 cTimeStamp CHAR(100),
 cDecimal   CHAR(100),
 cDouble    CHAR(100),
 cFloat     CHAR(100),
 cBigInt    CHAR(100),
 cInt       CHAR(100),
 cSmallInt  CHAR(100),
 cTinyint   CHAR(100),
 cBoolean   CHAR(100));

select cId, cTimeStamp from testAltColRCF_n0 order by cId;
select cId, cDecimal, cDouble, cFloat from testAltColRCF_n0 order by cId;
select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColRCF_n0 order by cId;
select cId, cBoolean from testAltColRCF_n0 order by cId;

alter table testAltColRCF_n0 replace columns
(cId        TINYINT,
 cTimeStamp VARCHAR(4),
 cDecimal   VARCHAR(4),
 cDouble    VARCHAR(4),
 cFloat     VARCHAR(4),
 cBigInt    VARCHAR(4),
 cInt       VARCHAR(4),
 cSmallInt  VARCHAR(4),
 cTinyint   VARCHAR(4),
 cBoolean   VARCHAR(4));

select cId, cTimeStamp from testAltColRCF_n0 order by cId;
select cId, cDecimal, cDouble, cFloat from testAltColRCF_n0 order by cId;
select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColRCF_n0 order by cId;
select cId, cBoolean from testAltColRCF_n0 order by cId;

alter table testAltColRCF_n0 replace columns
(cId        TINYINT,
 cTimeStamp CHAR(4),
 cDecimal   CHAR(4),
 cDouble    CHAR(4),
 cFloat     CHAR(4),
 cBigInt    CHAR(4),
 cInt       CHAR(4),
 cSmallInt  CHAR(4),
 cTinyint   CHAR(4),
 cBoolean   CHAR(4));

select cId, cTimeStamp from testAltColRCF_n0 order by cId;
select cId, cDecimal, cDouble, cFloat from testAltColRCF_n0 order by cId;
select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColRCF_n0 order by cId;
select cId, cBoolean from testAltColRCF_n0 order by cId;

drop table if exists testAltColRCF_n0;
-- RCFile type: End

-- Parquet type: Begin
drop table if exists testAltColP;
create table testAltColP stored as parquet as select * from testAltCol_n0;

select cId, cTimeStamp from testAltColP order by cId;
select cId, cDecimal, cDouble, cFloat from testAltColP order by cId;
select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColP order by cId;
select cId, cBoolean from testAltColP order by cId;

alter table testAltColP replace columns
(cId        TINYINT,
 cTimeStamp STRING,
 cDecimal   STRING,
 cDouble    STRING,
 cFloat     STRING,
 cBigInt    STRING,
 cInt       STRING,
 cSmallInt  STRING,
 cTinyint   STRING,
 cBoolean   STRING);

select cId, cTimeStamp from testAltColP order by cId;
select cId, cDecimal, cDouble, cFloat from testAltColP order by cId;
select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColP order by cId;
select cId, cBoolean from testAltColP order by cId;

alter table testAltColP replace columns
(cId        TINYINT,
 cTimeStamp VARCHAR(100),
 cDecimal   VARCHAR(100),
 cDouble    VARCHAR(100),
 cFloat     VARCHAR(100),
 cBigInt    VARCHAR(100),
 cInt       VARCHAR(100),
 cSmallInt  VARCHAR(100),
 cTinyint   VARCHAR(100),
 cBoolean   VARCHAR(100));

select cId, cTimeStamp from testAltColP order by cId;
select cId, cDecimal, cDouble, cFloat from testAltColP order by cId;
select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColP order by cId;
select cId, cBoolean from testAltColP order by cId;

alter table testAltColP replace columns
(cId        TINYINT,
 cTimeStamp CHAR(100),
 cDecimal   CHAR(100),
 cDouble    CHAR(100),
 cFloat     CHAR(100),
 cBigInt    CHAR(100),
 cInt       CHAR(100),
 cSmallInt  CHAR(100),
 cTinyint   CHAR(100),
 cBoolean   CHAR(100));

select cId, cTimeStamp from testAltColP order by cId;
select cId, cDecimal, cDouble, cFloat from testAltColP order by cId;
select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColP order by cId;
select cId, cBoolean from testAltColP order by cId;

alter table testAltColP replace columns
(cId        TINYINT,
 cTimeStamp VARCHAR(4),
 cDecimal   VARCHAR(4),
 cDouble    VARCHAR(4),
 cFloat     VARCHAR(4),
 cBigInt    VARCHAR(4),
 cInt       VARCHAR(4),
 cSmallInt  VARCHAR(4),
 cTinyint   VARCHAR(4),
 cBoolean   VARCHAR(4));

select cId, cTimeStamp from testAltColP order by cId;
select cId, cDecimal, cDouble, cFloat from testAltColP order by cId;
select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColP order by cId;
select cId, cBoolean from testAltColP order by cId;

alter table testAltColP replace columns
(cId        TINYINT,
 cTimeStamp CHAR(4),
 cDecimal   CHAR(4),
 cDouble    CHAR(4),
 cFloat     CHAR(4),
 cBigInt    CHAR(4),
 cInt       CHAR(4),
 cSmallInt  CHAR(4),
 cTinyint   CHAR(4),
 cBoolean   CHAR(4));

select cId, cTimeStamp from testAltColP order by cId;
select cId, cDecimal, cDouble, cFloat from testAltColP order by cId;
select cId, cBigInt, cInt, cSmallInt, cTinyint from testAltColP order by cId;
select cId, cBoolean from testAltColP order by cId;

drop table if exists testAltColP;
-- Parquet type: End

drop table if exists testAltCol_n0;