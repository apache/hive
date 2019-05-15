-- The data stored (text, sequence file, rc file, orc, parquet with and without dictionary
-- encryption) as Float, Double, Decimal and Number is read back as Bigint, int, smallint
-- tinyint, float, double, decimal and number.  This is done after the type is changed in HMS
-- through alter table.  Vectorization is disabled for this test.

-- Create a base table to be used for loading data: Begin
drop table if exists testAltCol_n2;
create table testAltCol_n2
(cId            TINYINT,
 cFloat         FLOAT,
 cDouble        DOUBLE,
 cDecimal38_18  DECIMAL(38,18),
 cDecimal38_37  DECIMAL(38,37),
 cDecimal16_8   DECIMAL(16,8),
 cDecimal3_2    DECIMAL(3,2),
 cNumeric38_18  NUMERIC(38,18),
 cNumeric38_37  NUMERIC(38,37),
 cNumeric16_8   NUMERIC(16,8),
 cNumeric3_2    NUMERIC(3,2));

insert into testAltCol_n2 values
(1,
 1.234e5,
 2.345e67,
 12345678901234567890.123456789012345678,
 1.2345678901234567890123456789012345678,
 12345678.90123456,
 1.23,
 12345678901234567890.123456789012345678,
 1.2345678901234567890123456789012345678,
 12345678.90123456,
 1.23),
(2,
 1.4e-45,
 4.94e-324,
 00000000000000000000.000000000000000001,
 0.0000000000000000000000000000000000001,
 00000000.00000001,
 0.01,
 00000000000000000000.000000000000000001,
 0.0000000000000000000000000000000000001,
 00000000.00000001,
 0.01),
(3,
 -1.4e-45,
 -4.94e-324,
 -00000000000000000000.000000000000000001,
 -0.0000000000000000000000000000000000001,
 -00000000.00000001,
 -0.01,
 -00000000000000000000.000000000000000001,
 -0.0000000000000000000000000000000000001,
 -00000000.00000001,
 -0.01),
(4,
 3.4e+38,
 1.79e+308,
 99999999999999999999.999999999999999999,
 9.9999999999999999999999999999999999999,
 99999999.99999999,
 9.99,
 99999999999999999999.999999999999999999,
 9.9999999999999999999999999999999999999,
 99999999.99999999,
 9.99),
(5,
 -3.4e+38,
 -1.79e+308,
 -99999999999999999999.999999999999999999,
 -9.9999999999999999999999999999999999999,
 -99999999.99999999,
 -9.99,
 -99999999999999999999.999999999999999999,
 -9.9999999999999999999999999999999999999,
 -99999999.99999999,
 -9.99),
(6,
 1,
 1,
 1,
 1,
 1,
 1,
 1,
 1,
 1,
 1),
(7,
 -1,
 -1,
 -1,
 -1,
 -1,
 -1,
 -1,
 -1,
 -1,
 -1),
(8,
 1234567890123456789,
 1234567890123456789,
 1234567890123456789,
 1,
 12345678,
 1,
 1234567890123456789,
 1,
 12345678,
 1),
(9,
 -1234567890123456789,
 -1234567890123456789,
 -1234567890123456789,
 -1,
 -12345678,
 -1,
 -1234567890123456789,
 -1,
 -12345678,
 -1),
(10,
 1234567890,
 1234567890,
 1234567890,
 1,
 12345678,
 1,
 1234567890,
 1,
 12345678,
 1),
(11,
 -1234567890,
 -1234567890,
 -1234567890,
 -1,
 -12345678,
 -1,
 -1234567890,
 -1,
 -12345678,
 -1),
(12,
 12345,
 12345,
 12345,
 1,
 12345,
 1,
 12345,
 1,
 12345,
 1),
(13,
 -12345,
 -12345,
 -12345,
 -1,
 -12345,
 -1,
 -12345,
 -1,
 -12345,
 -1),
(14,
 123,
 123,
 123,
 1,
 123,
 1,
 123,
 1,
 123,
 1),
(15,
 -123,
 -123,
 -123,
 -1,
 -123,
 -1,
 -123,
 -1,
 -123,
 -1),
(16,
 123456789e-8,
 234567890e-8,
 12345678.90123456,
 1.23456789,
 34567890.12345678,
 1.23,
 12345678.90123456,
 1.23456789,
 34567890.12345678,
 1.23),
(17,
 -123456789e-8,
 -234567890e-8,
 -12345678.90123456,
 -1.23456789,
 -34567890.12345678,
 -1.23,
 -12345678.90123456,
 -1.23456789,
 -34567890.12345678,
 -1.23),
(18,
 123456789e-2,
 234567890e-2,
 12345678.90,
 1.23,
 34567890.12,
 2.34,
 12345678.90,
 1.23,
 34567890.12,
 2.34),
(19,
 -123456789e-2,
 -234567890e-2,
 -12345678.90,
 -1.23,
 -34567890.12,
 -2.34,
 -12345678.90,
 -1.23,
 -34567890.12,
 -2.34);

select cId, cFloat, cDouble from testAltCol_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltCol_n2 order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltCol_n2 order by cId;
-- Create a base table to be used for loading data: End

-- Enable change of column type
SET hive.metastore.disallow.incompatible.col.type.changes=false;

-- Disable vectorization
SET hive.vectorized.execution.enabled=false;

-- Text type: Begin
drop table if exists testAltColT_n2;

create table testAltColT_n2 stored as textfile as select * from testAltCol_n2;

select cId, cFloat, cDouble from testAltColT_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColT_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColT_n2
order by cId;

alter table testAltColT_n2 replace columns
(cId            TINYINT,
 cFloat         FLOAT,
 cDouble        FLOAT,
 cDecimal38_18  FLOAT,
 cDecimal38_37  FLOAT,
 cDecimal16_8   FLOAT,
 cDecimal3_2    FLOAT,
 cNumeric38_18  FLOAT,
 cNumeric38_37  FLOAT,
 cNumeric16_8   FLOAT,
 cNumeric3_2    FLOAT);

select cId, cFloat, cDouble from testAltColT_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColT_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColT_n2
order by cId;

alter table testAltColT_n2 replace columns
(cId            TINYINT,
 cFloat         DOUBLE,
 cDouble        DOUBLE,
 cDecimal38_18  DOUBLE,
 cDecimal38_37  DOUBLE,
 cDecimal16_8   DOUBLE,
 cDecimal3_2    DOUBLE,
 cNumeric38_18  DOUBLE,
 cNumeric38_37  DOUBLE,
 cNumeric16_8   DOUBLE,
 cNumeric3_2    DOUBLE);

select cId, cFloat, cDouble from testAltColT_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColT_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColT_n2
order by cId;

alter table testAltColT_n2 replace columns
(cId            TINYINT,
 cFloat         DECIMAL(38,18),
 cDouble        DECIMAL(38,18),
 cDecimal38_18  DECIMAL(38,18),
 cDecimal38_37  DECIMAL(38,18),
 cDecimal16_8   DECIMAL(38,18),
 cDecimal3_2    DECIMAL(38,18),
 cNumeric38_18  DECIMAL(38,18),
 cNumeric38_37  DECIMAL(38,18),
 cNumeric16_8   DECIMAL(38,18),
 cNumeric3_2    DECIMAL(38,18));

select cId, cFloat, cDouble from testAltColT_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColT_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColT_n2
order by cId;

alter table testAltColT_n2 replace columns
(cId            TINYINT,
 cFloat         DECIMAL(38,37),
 cDouble        DECIMAL(38,37),
 cDecimal38_18  DECIMAL(38,37),
 cDecimal38_37  DECIMAL(38,37),
 cDecimal16_8   DECIMAL(38,37),
 cDecimal3_2    DECIMAL(38,37),
 cNumeric38_18  DECIMAL(38,37),
 cNumeric38_37  DECIMAL(38,37),
 cNumeric16_8   DECIMAL(38,37),
 cNumeric3_2    DECIMAL(38,37));

select cId, cFloat, cDouble from testAltColT_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColT_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColT_n2
order by cId;

alter table testAltColT_n2 replace columns
(cId            TINYINT,
 cFloat         DECIMAL(16,8),
 cDouble        DECIMAL(16,8),
 cDecimal38_18  DECIMAL(16,8),
 cDecimal38_37  DECIMAL(16,8),
 cDecimal16_8   DECIMAL(16,8),
 cDecimal3_2    DECIMAL(16,8),
 cNumeric38_18  DECIMAL(16,8),
 cNumeric38_37  DECIMAL(16,8),
 cNumeric16_8   DECIMAL(16,8),
 cNumeric3_2    DECIMAL(16,8));

select cId, cFloat, cDouble from testAltColT_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColT_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColT_n2
order by cId;

alter table testAltColT_n2 replace columns
(cId            TINYINT,
 cFloat         DECIMAL(3,2),
 cDouble        DECIMAL(3,2),
 cDecimal38_18  DECIMAL(3,2),
 cDecimal38_37  DECIMAL(3,2),
 cDecimal16_8   DECIMAL(3,2),
 cDecimal3_2    DECIMAL(3,2),
 cNumeric38_18  DECIMAL(3,2),
 cNumeric38_37  DECIMAL(3,2),
 cNumeric16_8   DECIMAL(3,2),
 cNumeric3_2    DECIMAL(3,2));

select cId, cFloat, cDouble from testAltColT_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColT_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColT_n2
order by cId;

alter table testAltColT_n2 replace columns
(cId            TINYINT,
 cFloat         NUMERIC(38,18),
 cDouble        NUMERIC(38,18),
 cDecimal38_18  NUMERIC(38,18),
 cDecimal38_37  NUMERIC(38,18),
 cDecimal16_8   NUMERIC(38,18),
 cDecimal3_2    NUMERIC(38,18),
 cNumeric38_18  NUMERIC(38,18),
 cNumeric38_37  NUMERIC(38,18),
 cNumeric16_8   NUMERIC(38,18),
 cNumeric3_2    NUMERIC(38,18));

select cId, cFloat, cDouble from testAltColT_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColT_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColT_n2
order by cId;

alter table testAltColT_n2 replace columns
(cId            TINYINT,
 cFloat         NUMERIC(38,37),
 cDouble        NUMERIC(38,37),
 cDecimal38_18  NUMERIC(38,37),
 cDecimal38_37  NUMERIC(38,37),
 cDecimal16_8   NUMERIC(38,37),
 cDecimal3_2    NUMERIC(38,37),
 cNumeric38_18  NUMERIC(38,37),
 cNumeric38_37  NUMERIC(38,37),
 cNumeric16_8   NUMERIC(38,37),
 cNumeric3_2    NUMERIC(38,37));

select cId, cFloat, cDouble from testAltColT_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColT_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColT_n2
order by cId;

alter table testAltColT_n2 replace columns
(cId            TINYINT,
 cFloat         NUMERIC(16,8),
 cDouble        NUMERIC(16,8),
 cDecimal38_18  NUMERIC(16,8),
 cDecimal38_37  NUMERIC(16,8),
 cDecimal16_8   NUMERIC(16,8),
 cDecimal3_2    NUMERIC(16,8),
 cNumeric38_18  NUMERIC(16,8),
 cNumeric38_37  NUMERIC(16,8),
 cNumeric16_8   NUMERIC(16,8),
 cNumeric3_2    NUMERIC(16,8));

select cId, cFloat, cDouble from testAltColT_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColT_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColT_n2
order by cId;

alter table testAltColT_n2 replace columns
(cId            TINYINT,
 cFloat         NUMERIC(3,2),
 cDouble        NUMERIC(3,2),
 cDecimal38_18  NUMERIC(3,2),
 cDecimal38_37  NUMERIC(3,2),
 cDecimal16_8   NUMERIC(3,2),
 cDecimal3_2    NUMERIC(3,2),
 cNumeric38_18  NUMERIC(3,2),
 cNumeric38_37  NUMERIC(3,2),
 cNumeric16_8   NUMERIC(3,2),
 cNumeric3_2    NUMERIC(3,2));

select cId, cFloat, cDouble from testAltColT_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColT_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColT_n2
order by cId;

alter table testAltColT_n2 replace columns
(cId            TINYINT,
 cFloat         BIGINT,
 cDouble        BIGINT,
 cDecimal38_18  BIGINT,
 cDecimal38_37  BIGINT,
 cDecimal16_8   BIGINT,
 cDecimal3_2    BIGINT,
 cNumeric38_18  BIGINT,
 cNumeric38_37  BIGINT,
 cNumeric16_8   BIGINT,
 cNumeric3_2    BIGINT);

select cId, cFloat, cDouble from testAltColT_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColT_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColT_n2
order by cId;

alter table testAltColT_n2 replace columns
(cId            TINYINT,
 cFloat         INT,
 cDouble        INT,
 cDecimal38_18  INT,
 cDecimal38_37  INT,
 cDecimal16_8   INT,
 cDecimal3_2    INT,
 cNumeric38_18  INT,
 cNumeric38_37  INT,
 cNumeric16_8   INT,
 cNumeric3_2    INT);

select cId, cFloat, cDouble from testAltColT_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColT_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColT_n2
order by cId;

alter table testAltColT_n2 replace columns
(cId            TINYINT,
 cFloat         SMALLINT,
 cDouble        SMALLINT,
 cDecimal38_18  SMALLINT,
 cDecimal38_37  SMALLINT,
 cDecimal16_8   SMALLINT,
 cDecimal3_2    SMALLINT,
 cNumeric38_18  SMALLINT,
 cNumeric38_37  SMALLINT,
 cNumeric16_8   SMALLINT,
 cNumeric3_2    SMALLINT);

select cId, cFloat, cDouble from testAltColT_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColT_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColT_n2
order by cId;

alter table testAltColT_n2 replace columns
(cId            TINYINT,
 cFloat         TINYINT,
 cDouble        TINYINT,
 cDecimal38_18  TINYINT,
 cDecimal38_37  TINYINT,
 cDecimal16_8   TINYINT,
 cDecimal3_2    TINYINT,
 cNumeric38_18  TINYINT,
 cNumeric38_37  TINYINT,
 cNumeric16_8   TINYINT,
 cNumeric3_2    TINYINT);

select cId, cFloat, cDouble from testAltColT_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColT_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColT_n2
order by cId;

drop table if exists testAltColT_n2;
-- Text type: End

-- Sequence File type: Begin
drop table if exists testAltColSF_n2;

create table testAltColSF_n2 stored as sequencefile as select * from testAltCol_n2;

select cId, cFloat, cDouble from testAltColSF_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColSF_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColSF_n2
order by cId;

alter table testAltColSF_n2 replace columns
(cId            TINYINT,
 cFloat         FLOAT,
 cDouble        FLOAT,
 cDecimal38_18  FLOAT,
 cDecimal38_37  FLOAT,
 cDecimal16_8   FLOAT,
 cDecimal3_2    FLOAT,
 cNumeric38_18  FLOAT,
 cNumeric38_37  FLOAT,
 cNumeric16_8   FLOAT,
 cNumeric3_2    FLOAT);

select cId, cFloat, cDouble from testAltColSF_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColSF_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColSF_n2
order by cId;

alter table testAltColSF_n2 replace columns
(cId            TINYINT,
 cFloat         DOUBLE,
 cDouble        DOUBLE,
 cDecimal38_18  DOUBLE,
 cDecimal38_37  DOUBLE,
 cDecimal16_8   DOUBLE,
 cDecimal3_2    DOUBLE,
 cNumeric38_18  DOUBLE,
 cNumeric38_37  DOUBLE,
 cNumeric16_8   DOUBLE,
 cNumeric3_2    DOUBLE);

select cId, cFloat, cDouble from testAltColSF_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColSF_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColSF_n2
order by cId;

alter table testAltColSF_n2 replace columns
(cId            TINYINT,
 cFloat         DECIMAL(38,18),
 cDouble        DECIMAL(38,18),
 cDecimal38_18  DECIMAL(38,18),
 cDecimal38_37  DECIMAL(38,18),
 cDecimal16_8   DECIMAL(38,18),
 cDecimal3_2    DECIMAL(38,18),
 cNumeric38_18  DECIMAL(38,18),
 cNumeric38_37  DECIMAL(38,18),
 cNumeric16_8   DECIMAL(38,18),
 cNumeric3_2    DECIMAL(38,18));

select cId, cFloat, cDouble from testAltColSF_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColSF_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColSF_n2
order by cId;

alter table testAltColSF_n2 replace columns
(cId            TINYINT,
 cFloat         DECIMAL(38,37),
 cDouble        DECIMAL(38,37),
 cDecimal38_18  DECIMAL(38,37),
 cDecimal38_37  DECIMAL(38,37),
 cDecimal16_8   DECIMAL(38,37),
 cDecimal3_2    DECIMAL(38,37),
 cNumeric38_18  DECIMAL(38,37),
 cNumeric38_37  DECIMAL(38,37),
 cNumeric16_8   DECIMAL(38,37),
 cNumeric3_2    DECIMAL(38,37));

select cId, cFloat, cDouble from testAltColSF_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColSF_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColSF_n2
order by cId;

alter table testAltColSF_n2 replace columns
(cId            TINYINT,
 cFloat         DECIMAL(16,8),
 cDouble        DECIMAL(16,8),
 cDecimal38_18  DECIMAL(16,8),
 cDecimal38_37  DECIMAL(16,8),
 cDecimal16_8   DECIMAL(16,8),
 cDecimal3_2    DECIMAL(16,8),
 cNumeric38_18  DECIMAL(16,8),
 cNumeric38_37  DECIMAL(16,8),
 cNumeric16_8   DECIMAL(16,8),
 cNumeric3_2    DECIMAL(16,8));

select cId, cFloat, cDouble from testAltColSF_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColSF_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColSF_n2
order by cId;

alter table testAltColSF_n2 replace columns
(cId            TINYINT,
 cFloat         DECIMAL(3,2),
 cDouble        DECIMAL(3,2),
 cDecimal38_18  DECIMAL(3,2),
 cDecimal38_37  DECIMAL(3,2),
 cDecimal16_8   DECIMAL(3,2),
 cDecimal3_2    DECIMAL(3,2),
 cNumeric38_18  DECIMAL(3,2),
 cNumeric38_37  DECIMAL(3,2),
 cNumeric16_8   DECIMAL(3,2),
 cNumeric3_2    DECIMAL(3,2));

select cId, cFloat, cDouble from testAltColSF_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColSF_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColSF_n2
order by cId;

alter table testAltColSF_n2 replace columns
(cId            TINYINT,
 cFloat         NUMERIC(38,18),
 cDouble        NUMERIC(38,18),
 cDecimal38_18  NUMERIC(38,18),
 cDecimal38_37  NUMERIC(38,18),
 cDecimal16_8   NUMERIC(38,18),
 cDecimal3_2    NUMERIC(38,18),
 cNumeric38_18  NUMERIC(38,18),
 cNumeric38_37  NUMERIC(38,18),
 cNumeric16_8   NUMERIC(38,18),
 cNumeric3_2    NUMERIC(38,18));

select cId, cFloat, cDouble from testAltColSF_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColSF_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColSF_n2
order by cId;

alter table testAltColSF_n2 replace columns
(cId            TINYINT,
 cFloat         NUMERIC(38,37),
 cDouble        NUMERIC(38,37),
 cDecimal38_18  NUMERIC(38,37),
 cDecimal38_37  NUMERIC(38,37),
 cDecimal16_8   NUMERIC(38,37),
 cDecimal3_2    NUMERIC(38,37),
 cNumeric38_18  NUMERIC(38,37),
 cNumeric38_37  NUMERIC(38,37),
 cNumeric16_8   NUMERIC(38,37),
 cNumeric3_2    NUMERIC(38,37));

select cId, cFloat, cDouble from testAltColSF_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColSF_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColSF_n2
order by cId;

alter table testAltColSF_n2 replace columns
(cId            TINYINT,
 cFloat         NUMERIC(16,8),
 cDouble        NUMERIC(16,8),
 cDecimal38_18  NUMERIC(16,8),
 cDecimal38_37  NUMERIC(16,8),
 cDecimal16_8   NUMERIC(16,8),
 cDecimal3_2    NUMERIC(16,8),
 cNumeric38_18  NUMERIC(16,8),
 cNumeric38_37  NUMERIC(16,8),
 cNumeric16_8   NUMERIC(16,8),
 cNumeric3_2    NUMERIC(16,8));

select cId, cFloat, cDouble from testAltColSF_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColSF_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColSF_n2
order by cId;

alter table testAltColSF_n2 replace columns
(cId            TINYINT,
 cFloat         NUMERIC(3,2),
 cDouble        NUMERIC(3,2),
 cDecimal38_18  NUMERIC(3,2),
 cDecimal38_37  NUMERIC(3,2),
 cDecimal16_8   NUMERIC(3,2),
 cDecimal3_2    NUMERIC(3,2),
 cNumeric38_18  NUMERIC(3,2),
 cNumeric38_37  NUMERIC(3,2),
 cNumeric16_8   NUMERIC(3,2),
 cNumeric3_2    NUMERIC(3,2));

select cId, cFloat, cDouble from testAltColSF_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColSF_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColSF_n2
order by cId;

alter table testAltColSF_n2 replace columns
(cId            TINYINT,
 cFloat         BIGINT,
 cDouble        BIGINT,
 cDecimal38_18  BIGINT,
 cDecimal38_37  BIGINT,
 cDecimal16_8   BIGINT,
 cDecimal3_2    BIGINT,
 cNumeric38_18  BIGINT,
 cNumeric38_37  BIGINT,
 cNumeric16_8   BIGINT,
 cNumeric3_2    BIGINT);

select cId, cFloat, cDouble from testAltColSF_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColSF_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColSF_n2
order by cId;

alter table testAltColSF_n2 replace columns
(cId            TINYINT,
 cFloat         INT,
 cDouble        INT,
 cDecimal38_18  INT,
 cDecimal38_37  INT,
 cDecimal16_8   INT,
 cDecimal3_2    INT,
 cNumeric38_18  INT,
 cNumeric38_37  INT,
 cNumeric16_8   INT,
 cNumeric3_2    INT);

select cId, cFloat, cDouble from testAltColSF_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColSF_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColSF_n2
order by cId;

alter table testAltColSF_n2 replace columns
(cId            TINYINT,
 cFloat         SMALLINT,
 cDouble        SMALLINT,
 cDecimal38_18  SMALLINT,
 cDecimal38_37  SMALLINT,
 cDecimal16_8   SMALLINT,
 cDecimal3_2    SMALLINT,
 cNumeric38_18  SMALLINT,
 cNumeric38_37  SMALLINT,
 cNumeric16_8   SMALLINT,
 cNumeric3_2    SMALLINT);

select cId, cFloat, cDouble from testAltColSF_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColSF_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColSF_n2
order by cId;

alter table testAltColSF_n2 replace columns
(cId            TINYINT,
 cFloat         TINYINT,
 cDouble        TINYINT,
 cDecimal38_18  TINYINT,
 cDecimal38_37  TINYINT,
 cDecimal16_8   TINYINT,
 cDecimal3_2    TINYINT,
 cNumeric38_18  TINYINT,
 cNumeric38_37  TINYINT,
 cNumeric16_8   TINYINT,
 cNumeric3_2    TINYINT);

select cId, cFloat, cDouble from testAltColSF_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColSF_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColSF_n2
order by cId;

drop table if exists testAltColSF_n2;
-- Sequence File type: End

-- RCFile type: Begin
drop table if exists testAltColRCF_n2;

create table testAltColRCF_n2 stored as rcfile as select * from testAltCol_n2;

select cId, cFloat, cDouble from testAltColRCF_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColRCF_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColRCF_n2
order by cId;

alter table testAltColRCF_n2 replace columns
(cId            TINYINT,
 cFloat         FLOAT,
 cDouble        FLOAT,
 cDecimal38_18  FLOAT,
 cDecimal38_37  FLOAT,
 cDecimal16_8   FLOAT,
 cDecimal3_2    FLOAT,
 cNumeric38_18  FLOAT,
 cNumeric38_37  FLOAT,
 cNumeric16_8   FLOAT,
 cNumeric3_2    FLOAT);

select cId, cFloat, cDouble from testAltColRCF_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColRCF_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColRCF_n2
order by cId;

alter table testAltColRCF_n2 replace columns
(cId            TINYINT,
 cFloat         DOUBLE,
 cDouble        DOUBLE,
 cDecimal38_18  DOUBLE,
 cDecimal38_37  DOUBLE,
 cDecimal16_8   DOUBLE,
 cDecimal3_2    DOUBLE,
 cNumeric38_18  DOUBLE,
 cNumeric38_37  DOUBLE,
 cNumeric16_8   DOUBLE,
 cNumeric3_2    DOUBLE);

select cId, cFloat, cDouble from testAltColRCF_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColRCF_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColRCF_n2
order by cId;

alter table testAltColRCF_n2 replace columns
(cId            TINYINT,
 cFloat         DECIMAL(38,18),
 cDouble        DECIMAL(38,18),
 cDecimal38_18  DECIMAL(38,18),
 cDecimal38_37  DECIMAL(38,18),
 cDecimal16_8   DECIMAL(38,18),
 cDecimal3_2    DECIMAL(38,18),
 cNumeric38_18  DECIMAL(38,18),
 cNumeric38_37  DECIMAL(38,18),
 cNumeric16_8   DECIMAL(38,18),
 cNumeric3_2    DECIMAL(38,18));

select cId, cFloat, cDouble from testAltColRCF_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColRCF_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColRCF_n2
order by cId;

alter table testAltColRCF_n2 replace columns
(cId            TINYINT,
 cFloat         DECIMAL(38,37),
 cDouble        DECIMAL(38,37),
 cDecimal38_18  DECIMAL(38,37),
 cDecimal38_37  DECIMAL(38,37),
 cDecimal16_8   DECIMAL(38,37),
 cDecimal3_2    DECIMAL(38,37),
 cNumeric38_18  DECIMAL(38,37),
 cNumeric38_37  DECIMAL(38,37),
 cNumeric16_8   DECIMAL(38,37),
 cNumeric3_2    DECIMAL(38,37));

select cId, cFloat, cDouble from testAltColRCF_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColRCF_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColRCF_n2
order by cId;

alter table testAltColRCF_n2 replace columns
(cId            TINYINT,
 cFloat         DECIMAL(16,8),
 cDouble        DECIMAL(16,8),
 cDecimal38_18  DECIMAL(16,8),
 cDecimal38_37  DECIMAL(16,8),
 cDecimal16_8   DECIMAL(16,8),
 cDecimal3_2    DECIMAL(16,8),
 cNumeric38_18  DECIMAL(16,8),
 cNumeric38_37  DECIMAL(16,8),
 cNumeric16_8   DECIMAL(16,8),
 cNumeric3_2    DECIMAL(16,8));

select cId, cFloat, cDouble from testAltColRCF_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColRCF_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColRCF_n2
order by cId;

alter table testAltColRCF_n2 replace columns
(cId            TINYINT,
 cFloat         DECIMAL(3,2),
 cDouble        DECIMAL(3,2),
 cDecimal38_18  DECIMAL(3,2),
 cDecimal38_37  DECIMAL(3,2),
 cDecimal16_8   DECIMAL(3,2),
 cDecimal3_2    DECIMAL(3,2),
 cNumeric38_18  DECIMAL(3,2),
 cNumeric38_37  DECIMAL(3,2),
 cNumeric16_8   DECIMAL(3,2),
 cNumeric3_2    DECIMAL(3,2));

select cId, cFloat, cDouble from testAltColRCF_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColRCF_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColRCF_n2
order by cId;

alter table testAltColRCF_n2 replace columns
(cId            TINYINT,
 cFloat         NUMERIC(38,18),
 cDouble        NUMERIC(38,18),
 cDecimal38_18  NUMERIC(38,18),
 cDecimal38_37  NUMERIC(38,18),
 cDecimal16_8   NUMERIC(38,18),
 cDecimal3_2    NUMERIC(38,18),
 cNumeric38_18  NUMERIC(38,18),
 cNumeric38_37  NUMERIC(38,18),
 cNumeric16_8   NUMERIC(38,18),
 cNumeric3_2    NUMERIC(38,18));

select cId, cFloat, cDouble from testAltColRCF_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColRCF_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColRCF_n2
order by cId;

alter table testAltColRCF_n2 replace columns
(cId            TINYINT,
 cFloat         NUMERIC(38,37),
 cDouble        NUMERIC(38,37),
 cDecimal38_18  NUMERIC(38,37),
 cDecimal38_37  NUMERIC(38,37),
 cDecimal16_8   NUMERIC(38,37),
 cDecimal3_2    NUMERIC(38,37),
 cNumeric38_18  NUMERIC(38,37),
 cNumeric38_37  NUMERIC(38,37),
 cNumeric16_8   NUMERIC(38,37),
 cNumeric3_2    NUMERIC(38,37));

select cId, cFloat, cDouble from testAltColRCF_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColRCF_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColRCF_n2
order by cId;

alter table testAltColRCF_n2 replace columns
(cId            TINYINT,
 cFloat         NUMERIC(16,8),
 cDouble        NUMERIC(16,8),
 cDecimal38_18  NUMERIC(16,8),
 cDecimal38_37  NUMERIC(16,8),
 cDecimal16_8   NUMERIC(16,8),
 cDecimal3_2    NUMERIC(16,8),
 cNumeric38_18  NUMERIC(16,8),
 cNumeric38_37  NUMERIC(16,8),
 cNumeric16_8   NUMERIC(16,8),
 cNumeric3_2    NUMERIC(16,8));

select cId, cFloat, cDouble from testAltColRCF_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColRCF_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColRCF_n2
order by cId;

alter table testAltColRCF_n2 replace columns
(cId            TINYINT,
 cFloat         NUMERIC(3,2),
 cDouble        NUMERIC(3,2),
 cDecimal38_18  NUMERIC(3,2),
 cDecimal38_37  NUMERIC(3,2),
 cDecimal16_8   NUMERIC(3,2),
 cDecimal3_2    NUMERIC(3,2),
 cNumeric38_18  NUMERIC(3,2),
 cNumeric38_37  NUMERIC(3,2),
 cNumeric16_8   NUMERIC(3,2),
 cNumeric3_2    NUMERIC(3,2));

select cId, cFloat, cDouble from testAltColRCF_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColRCF_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColRCF_n2
order by cId;

alter table testAltColRCF_n2 replace columns
(cId            TINYINT,
 cFloat         BIGINT,
 cDouble        BIGINT,
 cDecimal38_18  BIGINT,
 cDecimal38_37  BIGINT,
 cDecimal16_8   BIGINT,
 cDecimal3_2    BIGINT,
 cNumeric38_18  BIGINT,
 cNumeric38_37  BIGINT,
 cNumeric16_8   BIGINT,
 cNumeric3_2    BIGINT);

select cId, cFloat, cDouble from testAltColRCF_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColRCF_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColRCF_n2
order by cId;

alter table testAltColRCF_n2 replace columns
(cId            TINYINT,
 cFloat         INT,
 cDouble        INT,
 cDecimal38_18  INT,
 cDecimal38_37  INT,
 cDecimal16_8   INT,
 cDecimal3_2    INT,
 cNumeric38_18  INT,
 cNumeric38_37  INT,
 cNumeric16_8   INT,
 cNumeric3_2    INT);

select cId, cFloat, cDouble from testAltColRCF_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColRCF_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColRCF_n2
order by cId;

alter table testAltColRCF_n2 replace columns
(cId            TINYINT,
 cFloat         SMALLINT,
 cDouble        SMALLINT,
 cDecimal38_18  SMALLINT,
 cDecimal38_37  SMALLINT,
 cDecimal16_8   SMALLINT,
 cDecimal3_2    SMALLINT,
 cNumeric38_18  SMALLINT,
 cNumeric38_37  SMALLINT,
 cNumeric16_8   SMALLINT,
 cNumeric3_2    SMALLINT);

select cId, cFloat, cDouble from testAltColRCF_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColRCF_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColRCF_n2
order by cId;

alter table testAltColRCF_n2 replace columns
(cId            TINYINT,
 cFloat         TINYINT,
 cDouble        TINYINT,
 cDecimal38_18  TINYINT,
 cDecimal38_37  TINYINT,
 cDecimal16_8   TINYINT,
 cDecimal3_2    TINYINT,
 cNumeric38_18  TINYINT,
 cNumeric38_37  TINYINT,
 cNumeric16_8   TINYINT,
 cNumeric3_2    TINYINT);

select cId, cFloat, cDouble from testAltColRCF_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColRCF_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColRCF_n2
order by cId;

drop table if exists testAltColRCF_n2;
-- RCFile type: End

-- ORC type: Begin
drop table if exists testAltColORC_n2;

create table testAltColORC_n2 stored as orc as select * from testAltCol_n2;

select cId, cFloat, cDouble from testAltColORC_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColORC_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColORC_n2
order by cId;

alter table testAltColORC_n2 replace columns
(cId            TINYINT,
 cFloat         FLOAT,
 cDouble        FLOAT,
 cDecimal38_18  FLOAT,
 cDecimal38_37  FLOAT,
 cDecimal16_8   FLOAT,
 cDecimal3_2    FLOAT,
 cNumeric38_18  FLOAT,
 cNumeric38_37  FLOAT,
 cNumeric16_8   FLOAT,
 cNumeric3_2    FLOAT);

select cId, cFloat, cDouble from testAltColORC_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColORC_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColORC_n2
order by cId;

alter table testAltColORC_n2 replace columns
(cId            TINYINT,
 cFloat         DOUBLE,
 cDouble        DOUBLE,
 cDecimal38_18  DOUBLE,
 cDecimal38_37  DOUBLE,
 cDecimal16_8   DOUBLE,
 cDecimal3_2    DOUBLE,
 cNumeric38_18  DOUBLE,
 cNumeric38_37  DOUBLE,
 cNumeric16_8   DOUBLE,
 cNumeric3_2    DOUBLE);

select cId, cFloat, cDouble from testAltColORC_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColORC_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColORC_n2
order by cId;

alter table testAltColORC_n2 replace columns
(cId            TINYINT,
 cFloat         DECIMAL(38,18),
 cDouble        DECIMAL(38,18),
 cDecimal38_18  DECIMAL(38,18),
 cDecimal38_37  DECIMAL(38,18),
 cDecimal16_8   DECIMAL(38,18),
 cDecimal3_2    DECIMAL(38,18),
 cNumeric38_18  DECIMAL(38,18),
 cNumeric38_37  DECIMAL(38,18),
 cNumeric16_8   DECIMAL(38,18),
 cNumeric3_2    DECIMAL(38,18));

select cId, cFloat, cDouble from testAltColORC_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColORC_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColORC_n2
order by cId;

alter table testAltColORC_n2 replace columns
(cId            TINYINT,
 cFloat         DECIMAL(38,37),
 cDouble        DECIMAL(38,37),
 cDecimal38_18  DECIMAL(38,37),
 cDecimal38_37  DECIMAL(38,37),
 cDecimal16_8   DECIMAL(38,37),
 cDecimal3_2    DECIMAL(38,37),
 cNumeric38_18  DECIMAL(38,37),
 cNumeric38_37  DECIMAL(38,37),
 cNumeric16_8   DECIMAL(38,37),
 cNumeric3_2    DECIMAL(38,37));

select cId, cFloat, cDouble from testAltColORC_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColORC_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColORC_n2
order by cId;

-- The following two cases should be enabled after a new version of ORC, that has the fix for
-- ORC-379 is picked up.
-- alter table testAltColORC_n2 replace columns
-- (cId            TINYINT,
--  cFloat         DECIMAL(16,8),
--  cDouble        DECIMAL(16,8),
--  cDecimal38_18  DECIMAL(16,8),
--  cDecimal38_37  DECIMAL(16,8),
--  cDecimal16_8   DECIMAL(16,8),
--  cDecimal3_2    DECIMAL(16,8),
--  cNumeric38_18  DECIMAL(16,8),
--  cNumeric38_37  DECIMAL(16,8),
--  cNumeric16_8   DECIMAL(16,8),
--  cNumeric3_2    DECIMAL(16,8));

-- select cId, cFloat, cDouble from testAltColORC_n2 order by cId;
-- select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColORC_n2
-- order by cId;
-- select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColORC_n2
-- order by cId;

-- alter table testAltColORC_n2 replace columns
-- (cId            TINYINT,
--  cFloat         DECIMAL(3,2),
--  cDouble        DECIMAL(3,2),
--  cDecimal38_18  DECIMAL(3,2),
--  cDecimal38_37  DECIMAL(3,2),
--  cDecimal16_8   DECIMAL(3,2),
--  cDecimal3_2    DECIMAL(3,2),
--  cNumeric38_18  DECIMAL(3,2),
--  cNumeric38_37  DECIMAL(3,2),
--  cNumeric16_8   DECIMAL(3,2),
--  cNumeric3_2    DECIMAL(3,2));

-- select cId, cFloat, cDouble from testAltColORC_n2 order by cId;
-- select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColORC_n2
-- order by cId;
-- select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColORC_n2
-- order by cId;

alter table testAltColORC_n2 replace columns
(cId            TINYINT,
 cFloat         NUMERIC(38,18),
 cDouble        NUMERIC(38,18),
 cDecimal38_18  NUMERIC(38,18),
 cDecimal38_37  NUMERIC(38,18),
 cDecimal16_8   NUMERIC(38,18),
 cDecimal3_2    NUMERIC(38,18),
 cNumeric38_18  NUMERIC(38,18),
 cNumeric38_37  NUMERIC(38,18),
 cNumeric16_8   NUMERIC(38,18),
 cNumeric3_2    NUMERIC(38,18));

select cId, cFloat, cDouble from testAltColORC_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColORC_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColORC_n2
order by cId;

alter table testAltColORC_n2 replace columns
(cId            TINYINT,
 cFloat         NUMERIC(38,37),
 cDouble        NUMERIC(38,37),
 cDecimal38_18  NUMERIC(38,37),
 cDecimal38_37  NUMERIC(38,37),
 cDecimal16_8   NUMERIC(38,37),
 cDecimal3_2    NUMERIC(38,37),
 cNumeric38_18  NUMERIC(38,37),
 cNumeric38_37  NUMERIC(38,37),
 cNumeric16_8   NUMERIC(38,37),
 cNumeric3_2    NUMERIC(38,37));

select cId, cFloat, cDouble from testAltColORC_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColORC_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColORC_n2
order by cId;

-- The following two cases should be enabled after a new version of ORC, that has the fix for
-- ORA-379 is picked up.
-- alter table testAltColORC_n2 replace columns
-- (cId            TINYINT,
--  cFloat         NUMERIC(16,8),
--  cDouble        NUMERIC(16,8),
--  cDecimal38_18  NUMERIC(16,8),
--  cDecimal38_37  NUMERIC(16,8),
--  cDecimal16_8   NUMERIC(16,8),
--  cDecimal3_2    NUMERIC(16,8),
--  cNumeric38_18  NUMERIC(16,8),
--  cNumeric38_37  NUMERIC(16,8),
--  cNumeric16_8   NUMERIC(16,8),
--  cNumeric3_2    NUMERIC(16,8));

-- select cId, cFloat, cDouble from testAltColORC_n2 order by cId;
-- select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColORC_n2
-- order by cId;
-- select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColORC_n2
-- order by cId;

-- alter table testAltColORC_n2 replace columns
-- (cId            TINYINT,
--  cFloat         NUMERIC(3,2),
--  cDouble        NUMERIC(3,2),
--  cDecimal38_18  NUMERIC(3,2),
--  cDecimal38_37  NUMERIC(3,2),
--  cDecimal16_8   NUMERIC(3,2),
--  cDecimal3_2    NUMERIC(3,2),
--  cNumeric38_18  NUMERIC(3,2),
--  cNumeric38_37  NUMERIC(3,2),
--  cNumeric16_8   NUMERIC(3,2),
--  cNumeric3_2    NUMERIC(3,2));

-- select cId, cFloat, cDouble from testAltColORC_n2 order by cId;
-- select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColORC_n2
-- order by cId;
-- select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColORC_n2
-- order by cId;

alter table testAltColORC_n2 replace columns
(cId            TINYINT,
 cFloat         BIGINT,
 cDouble        BIGINT,
 cDecimal38_18  BIGINT,
 cDecimal38_37  BIGINT,
 cDecimal16_8   BIGINT,
 cDecimal3_2    BIGINT,
 cNumeric38_18  BIGINT,
 cNumeric38_37  BIGINT,
 cNumeric16_8   BIGINT,
 cNumeric3_2    BIGINT);

select cId, cFloat, cDouble from testAltColORC_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColORC_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColORC_n2
order by cId;

alter table testAltColORC_n2 replace columns
(cId            TINYINT,
 cFloat         INT,
 cDouble        INT,
 cDecimal38_18  INT,
 cDecimal38_37  INT,
 cDecimal16_8   INT,
 cDecimal3_2    INT,
 cNumeric38_18  INT,
 cNumeric38_37  INT,
 cNumeric16_8   INT,
 cNumeric3_2    INT);

select cId, cFloat, cDouble from testAltColORC_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColORC_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColORC_n2
order by cId;

alter table testAltColORC_n2 replace columns
(cId            TINYINT,
 cFloat         SMALLINT,
 cDouble        SMALLINT,
 cDecimal38_18  SMALLINT,
 cDecimal38_37  SMALLINT,
 cDecimal16_8   SMALLINT,
 cDecimal3_2    SMALLINT,
 cNumeric38_18  SMALLINT,
 cNumeric38_37  SMALLINT,
 cNumeric16_8   SMALLINT,
 cNumeric3_2    SMALLINT);

select cId, cFloat, cDouble from testAltColORC_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColORC_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColORC_n2
order by cId;

alter table testAltColORC_n2 replace columns
(cId            TINYINT,
 cFloat         TINYINT,
 cDouble        TINYINT,
 cDecimal38_18  TINYINT,
 cDecimal38_37  TINYINT,
 cDecimal16_8   TINYINT,
 cDecimal3_2    TINYINT,
 cNumeric38_18  TINYINT,
 cNumeric38_37  TINYINT,
 cNumeric16_8   TINYINT,
 cNumeric3_2    TINYINT);

select cId, cFloat, cDouble from testAltColORC_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColORC_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColORC_n2
order by cId;

drop table if exists testAltColORC_n2;
-- ORC type: End

-- Parquet type with Dictionary encoding enabled: Begin
drop table if exists testAltColPDE_n2;

create table testAltColPDE_n2 stored as parquet as select * from testAltCol_n2;

select cId, cFloat, cDouble from testAltColPDE_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColPDE_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColPDE_n2
order by cId;

alter table testAltColPDE_n2 replace columns
(cId            TINYINT,
 cFloat         FLOAT,
 cDouble        FLOAT,
 cDecimal38_18  FLOAT,
 cDecimal38_37  FLOAT,
 cDecimal16_8   FLOAT,
 cDecimal3_2    FLOAT,
 cNumeric38_18  FLOAT,
 cNumeric38_37  FLOAT,
 cNumeric16_8   FLOAT,
 cNumeric3_2    FLOAT);

select cId, cFloat, cDouble from testAltColPDE_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColPDE_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColPDE_n2
order by cId;

alter table testAltColPDE_n2 replace columns
(cId            TINYINT,
 cFloat         DOUBLE,
 cDouble        DOUBLE,
 cDecimal38_18  DOUBLE,
 cDecimal38_37  DOUBLE,
 cDecimal16_8   DOUBLE,
 cDecimal3_2    DOUBLE,
 cNumeric38_18  DOUBLE,
 cNumeric38_37  DOUBLE,
 cNumeric16_8   DOUBLE,
 cNumeric3_2    DOUBLE);

select cId, cFloat, cDouble from testAltColPDE_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColPDE_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColPDE_n2
order by cId;

alter table testAltColPDE_n2 replace columns
(cId            TINYINT,
 cFloat         DECIMAL(38,18),
 cDouble        DECIMAL(38,18),
 cDecimal38_18  DECIMAL(38,18),
 cDecimal38_37  DECIMAL(38,18),
 cDecimal16_8   DECIMAL(38,18),
 cDecimal3_2    DECIMAL(38,18),
 cNumeric38_18  DECIMAL(38,18),
 cNumeric38_37  DECIMAL(38,18),
 cNumeric16_8   DECIMAL(38,18),
 cNumeric3_2    DECIMAL(38,18));

select cId, cFloat, cDouble from testAltColPDE_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColPDE_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColPDE_n2
order by cId;

alter table testAltColPDE_n2 replace columns
(cId            TINYINT,
 cFloat         DECIMAL(38,37),
 cDouble        DECIMAL(38,37),
 cDecimal38_18  DECIMAL(38,37),
 cDecimal38_37  DECIMAL(38,37),
 cDecimal16_8   DECIMAL(38,37),
 cDecimal3_2    DECIMAL(38,37),
 cNumeric38_18  DECIMAL(38,37),
 cNumeric38_37  DECIMAL(38,37),
 cNumeric16_8   DECIMAL(38,37),
 cNumeric3_2    DECIMAL(38,37));

select cId, cFloat, cDouble from testAltColPDE_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColPDE_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColPDE_n2
order by cId;

alter table testAltColPDE_n2 replace columns
(cId            TINYINT,
 cFloat         DECIMAL(16,8),
 cDouble        DECIMAL(16,8),
 cDecimal38_18  DECIMAL(16,8),
 cDecimal38_37  DECIMAL(16,8),
 cDecimal16_8   DECIMAL(16,8),
 cDecimal3_2    DECIMAL(16,8),
 cNumeric38_18  DECIMAL(16,8),
 cNumeric38_37  DECIMAL(16,8),
 cNumeric16_8   DECIMAL(16,8),
 cNumeric3_2    DECIMAL(16,8));

select cId, cFloat, cDouble from testAltColPDE_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColPDE_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColPDE_n2
order by cId;

alter table testAltColPDE_n2 replace columns
(cId            TINYINT,
 cFloat         DECIMAL(3,2),
 cDouble        DECIMAL(3,2),
 cDecimal38_18  DECIMAL(3,2),
 cDecimal38_37  DECIMAL(3,2),
 cDecimal16_8   DECIMAL(3,2),
 cDecimal3_2    DECIMAL(3,2),
 cNumeric38_18  DECIMAL(3,2),
 cNumeric38_37  DECIMAL(3,2),
 cNumeric16_8   DECIMAL(3,2),
 cNumeric3_2    DECIMAL(3,2));

select cId, cFloat, cDouble from testAltColPDE_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColPDE_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColPDE_n2
order by cId;

alter table testAltColPDE_n2 replace columns
(cId            TINYINT,
 cFloat         NUMERIC(38,18),
 cDouble        NUMERIC(38,18),
 cDecimal38_18  NUMERIC(38,18),
 cDecimal38_37  NUMERIC(38,18),
 cDecimal16_8   NUMERIC(38,18),
 cDecimal3_2    NUMERIC(38,18),
 cNumeric38_18  NUMERIC(38,18),
 cNumeric38_37  NUMERIC(38,18),
 cNumeric16_8   NUMERIC(38,18),
 cNumeric3_2    NUMERIC(38,18));

select cId, cFloat, cDouble from testAltColPDE_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColPDE_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColPDE_n2
order by cId;

alter table testAltColPDE_n2 replace columns
(cId            TINYINT,
 cFloat         NUMERIC(38,37),
 cDouble        NUMERIC(38,37),
 cDecimal38_18  NUMERIC(38,37),
 cDecimal38_37  NUMERIC(38,37),
 cDecimal16_8   NUMERIC(38,37),
 cDecimal3_2    NUMERIC(38,37),
 cNumeric38_18  NUMERIC(38,37),
 cNumeric38_37  NUMERIC(38,37),
 cNumeric16_8   NUMERIC(38,37),
 cNumeric3_2    NUMERIC(38,37));

select cId, cFloat, cDouble from testAltColPDE_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColPDE_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColPDE_n2
order by cId;

alter table testAltColPDE_n2 replace columns
(cId            TINYINT,
 cFloat         NUMERIC(16,8),
 cDouble        NUMERIC(16,8),
 cDecimal38_18  NUMERIC(16,8),
 cDecimal38_37  NUMERIC(16,8),
 cDecimal16_8   NUMERIC(16,8),
 cDecimal3_2    NUMERIC(16,8),
 cNumeric38_18  NUMERIC(16,8),
 cNumeric38_37  NUMERIC(16,8),
 cNumeric16_8   NUMERIC(16,8),
 cNumeric3_2    NUMERIC(16,8));

select cId, cFloat, cDouble from testAltColPDE_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColPDE_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColPDE_n2
order by cId;

alter table testAltColPDE_n2 replace columns
(cId            TINYINT,
 cFloat         NUMERIC(3,2),
 cDouble        NUMERIC(3,2),
 cDecimal38_18  NUMERIC(3,2),
 cDecimal38_37  NUMERIC(3,2),
 cDecimal16_8   NUMERIC(3,2),
 cDecimal3_2    NUMERIC(3,2),
 cNumeric38_18  NUMERIC(3,2),
 cNumeric38_37  NUMERIC(3,2),
 cNumeric16_8   NUMERIC(3,2),
 cNumeric3_2    NUMERIC(3,2));

select cId, cFloat, cDouble from testAltColPDE_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColPDE_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColPDE_n2
order by cId;

alter table testAltColPDE_n2 replace columns
(cId            TINYINT,
 cFloat         BIGINT,
 cDouble        BIGINT,
 cDecimal38_18  BIGINT,
 cDecimal38_37  BIGINT,
 cDecimal16_8   BIGINT,
 cDecimal3_2    BIGINT,
 cNumeric38_18  BIGINT,
 cNumeric38_37  BIGINT,
 cNumeric16_8   BIGINT,
 cNumeric3_2    BIGINT);

select cId, cFloat, cDouble from testAltColPDE_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColPDE_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColPDE_n2
order by cId;

alter table testAltColPDE_n2 replace columns
(cId            TINYINT,
 cFloat         INT,
 cDouble        INT,
 cDecimal38_18  INT,
 cDecimal38_37  INT,
 cDecimal16_8   INT,
 cDecimal3_2    INT,
 cNumeric38_18  INT,
 cNumeric38_37  INT,
 cNumeric16_8   INT,
 cNumeric3_2    INT);

select cId, cFloat, cDouble from testAltColPDE_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColPDE_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColPDE_n2
order by cId;

alter table testAltColPDE_n2 replace columns
(cId            TINYINT,
 cFloat         SMALLINT,
 cDouble        SMALLINT,
 cDecimal38_18  SMALLINT,
 cDecimal38_37  SMALLINT,
 cDecimal16_8   SMALLINT,
 cDecimal3_2    SMALLINT,
 cNumeric38_18  SMALLINT,
 cNumeric38_37  SMALLINT,
 cNumeric16_8   SMALLINT,
 cNumeric3_2    SMALLINT);

select cId, cFloat, cDouble from testAltColPDE_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColPDE_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColPDE_n2
order by cId;

alter table testAltColPDE_n2 replace columns
(cId            TINYINT,
 cFloat         TINYINT,
 cDouble        TINYINT,
 cDecimal38_18  TINYINT,
 cDecimal38_37  TINYINT,
 cDecimal16_8   TINYINT,
 cDecimal3_2    TINYINT,
 cNumeric38_18  TINYINT,
 cNumeric38_37  TINYINT,
 cNumeric16_8   TINYINT,
 cNumeric3_2    TINYINT);

select cId, cFloat, cDouble from testAltColPDE_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColPDE_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColPDE_n2
order by cId;

drop table if exists testAltColPDE_n2;
-- Parquet type with Dictionary encoding enabled: End

-- Parquet type with Dictionary encoding disabled: Begin
drop table if exists testAltColPDD_n2;

create table testAltColPDD_n2 stored as parquet tblproperties ("parquet.enable.dictionary"="false") as
select * from testAltCol_n2;

select cId, cFloat, cDouble from testAltColPDD_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColPDD_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColPDD_n2
order by cId;

alter table testAltColPDD_n2 replace columns
(cId            TINYINT,
 cFloat         FLOAT,
 cDouble        FLOAT,
 cDecimal38_18  FLOAT,
 cDecimal38_37  FLOAT,
 cDecimal16_8   FLOAT,
 cDecimal3_2    FLOAT,
 cNumeric38_18  FLOAT,
 cNumeric38_37  FLOAT,
 cNumeric16_8   FLOAT,
 cNumeric3_2    FLOAT);

select cId, cFloat, cDouble from testAltColPDD_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColPDD_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColPDD_n2
order by cId;

alter table testAltColPDD_n2 replace columns
(cId            TINYINT,
 cFloat         DOUBLE,
 cDouble        DOUBLE,
 cDecimal38_18  DOUBLE,
 cDecimal38_37  DOUBLE,
 cDecimal16_8   DOUBLE,
 cDecimal3_2    DOUBLE,
 cNumeric38_18  DOUBLE,
 cNumeric38_37  DOUBLE,
 cNumeric16_8   DOUBLE,
 cNumeric3_2    DOUBLE);

select cId, cFloat, cDouble from testAltColPDD_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColPDD_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColPDD_n2
order by cId;

alter table testAltColPDD_n2 replace columns
(cId            TINYINT,
 cFloat         DECIMAL(38,18),
 cDouble        DECIMAL(38,18),
 cDecimal38_18  DECIMAL(38,18),
 cDecimal38_37  DECIMAL(38,18),
 cDecimal16_8   DECIMAL(38,18),
 cDecimal3_2    DECIMAL(38,18),
 cNumeric38_18  DECIMAL(38,18),
 cNumeric38_37  DECIMAL(38,18),
 cNumeric16_8   DECIMAL(38,18),
 cNumeric3_2    DECIMAL(38,18));

select cId, cFloat, cDouble from testAltColPDD_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColPDD_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColPDD_n2
order by cId;

alter table testAltColPDD_n2 replace columns
(cId            TINYINT,
 cFloat         DECIMAL(38,37),
 cDouble        DECIMAL(38,37),
 cDecimal38_18  DECIMAL(38,37),
 cDecimal38_37  DECIMAL(38,37),
 cDecimal16_8   DECIMAL(38,37),
 cDecimal3_2    DECIMAL(38,37),
 cNumeric38_18  DECIMAL(38,37),
 cNumeric38_37  DECIMAL(38,37),
 cNumeric16_8   DECIMAL(38,37),
 cNumeric3_2    DECIMAL(38,37));

select cId, cFloat, cDouble from testAltColPDD_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColPDD_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColPDD_n2
order by cId;

alter table testAltColPDD_n2 replace columns
(cId            TINYINT,
 cFloat         DECIMAL(16,8),
 cDouble        DECIMAL(16,8),
 cDecimal38_18  DECIMAL(16,8),
 cDecimal38_37  DECIMAL(16,8),
 cDecimal16_8   DECIMAL(16,8),
 cDecimal3_2    DECIMAL(16,8),
 cNumeric38_18  DECIMAL(16,8),
 cNumeric38_37  DECIMAL(16,8),
 cNumeric16_8   DECIMAL(16,8),
 cNumeric3_2    DECIMAL(16,8));

select cId, cFloat, cDouble from testAltColPDD_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColPDD_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColPDD_n2
order by cId;

alter table testAltColPDD_n2 replace columns
(cId            TINYINT,
 cFloat         DECIMAL(3,2),
 cDouble        DECIMAL(3,2),
 cDecimal38_18  DECIMAL(3,2),
 cDecimal38_37  DECIMAL(3,2),
 cDecimal16_8   DECIMAL(3,2),
 cDecimal3_2    DECIMAL(3,2),
 cNumeric38_18  DECIMAL(3,2),
 cNumeric38_37  DECIMAL(3,2),
 cNumeric16_8   DECIMAL(3,2),
 cNumeric3_2    DECIMAL(3,2));

select cId, cFloat, cDouble from testAltColPDD_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColPDD_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColPDD_n2
order by cId;

alter table testAltColPDD_n2 replace columns
(cId            TINYINT,
 cFloat         NUMERIC(38,18),
 cDouble        NUMERIC(38,18),
 cDecimal38_18  NUMERIC(38,18),
 cDecimal38_37  NUMERIC(38,18),
 cDecimal16_8   NUMERIC(38,18),
 cDecimal3_2    NUMERIC(38,18),
 cNumeric38_18  NUMERIC(38,18),
 cNumeric38_37  NUMERIC(38,18),
 cNumeric16_8   NUMERIC(38,18),
 cNumeric3_2    NUMERIC(38,18));

select cId, cFloat, cDouble from testAltColPDD_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColPDD_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColPDD_n2
order by cId;

alter table testAltColPDD_n2 replace columns
(cId            TINYINT,
 cFloat         NUMERIC(38,37),
 cDouble        NUMERIC(38,37),
 cDecimal38_18  NUMERIC(38,37),
 cDecimal38_37  NUMERIC(38,37),
 cDecimal16_8   NUMERIC(38,37),
 cDecimal3_2    NUMERIC(38,37),
 cNumeric38_18  NUMERIC(38,37),
 cNumeric38_37  NUMERIC(38,37),
 cNumeric16_8   NUMERIC(38,37),
 cNumeric3_2    NUMERIC(38,37));

select cId, cFloat, cDouble from testAltColPDD_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColPDD_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColPDD_n2
order by cId;

alter table testAltColPDD_n2 replace columns
(cId            TINYINT,
 cFloat         NUMERIC(16,8),
 cDouble        NUMERIC(16,8),
 cDecimal38_18  NUMERIC(16,8),
 cDecimal38_37  NUMERIC(16,8),
 cDecimal16_8   NUMERIC(16,8),
 cDecimal3_2    NUMERIC(16,8),
 cNumeric38_18  NUMERIC(16,8),
 cNumeric38_37  NUMERIC(16,8),
 cNumeric16_8   NUMERIC(16,8),
 cNumeric3_2    NUMERIC(16,8));

select cId, cFloat, cDouble from testAltColPDD_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColPDD_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColPDD_n2
order by cId;

alter table testAltColPDD_n2 replace columns
(cId            TINYINT,
 cFloat         NUMERIC(3,2),
 cDouble        NUMERIC(3,2),
 cDecimal38_18  NUMERIC(3,2),
 cDecimal38_37  NUMERIC(3,2),
 cDecimal16_8   NUMERIC(3,2),
 cDecimal3_2    NUMERIC(3,2),
 cNumeric38_18  NUMERIC(3,2),
 cNumeric38_37  NUMERIC(3,2),
 cNumeric16_8   NUMERIC(3,2),
 cNumeric3_2    NUMERIC(3,2));

select cId, cFloat, cDouble from testAltColPDD_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColPDD_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColPDD_n2
order by cId;

alter table testAltColPDD_n2 replace columns
(cId            TINYINT,
 cFloat         BIGINT,
 cDouble        BIGINT,
 cDecimal38_18  BIGINT,
 cDecimal38_37  BIGINT,
 cDecimal16_8   BIGINT,
 cDecimal3_2    BIGINT,
 cNumeric38_18  BIGINT,
 cNumeric38_37  BIGINT,
 cNumeric16_8   BIGINT,
 cNumeric3_2    BIGINT);

select cId, cFloat, cDouble from testAltColPDD_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColPDD_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColPDD_n2
order by cId;

alter table testAltColPDD_n2 replace columns
(cId            TINYINT,
 cFloat         INT,
 cDouble        INT,
 cDecimal38_18  INT,
 cDecimal38_37  INT,
 cDecimal16_8   INT,
 cDecimal3_2    INT,
 cNumeric38_18  INT,
 cNumeric38_37  INT,
 cNumeric16_8   INT,
 cNumeric3_2    INT);

select cId, cFloat, cDouble from testAltColPDD_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColPDD_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColPDD_n2
order by cId;

alter table testAltColPDD_n2 replace columns
(cId            TINYINT,
 cFloat         SMALLINT,
 cDouble        SMALLINT,
 cDecimal38_18  SMALLINT,
 cDecimal38_37  SMALLINT,
 cDecimal16_8   SMALLINT,
 cDecimal3_2    SMALLINT,
 cNumeric38_18  SMALLINT,
 cNumeric38_37  SMALLINT,
 cNumeric16_8   SMALLINT,
 cNumeric3_2    SMALLINT);

select cId, cFloat, cDouble from testAltColPDD_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColPDD_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColPDD_n2
order by cId;

alter table testAltColPDD_n2 replace columns
(cId            TINYINT,
 cFloat         TINYINT,
 cDouble        TINYINT,
 cDecimal38_18  TINYINT,
 cDecimal38_37  TINYINT,
 cDecimal16_8   TINYINT,
 cDecimal3_2    TINYINT,
 cNumeric38_18  TINYINT,
 cNumeric38_37  TINYINT,
 cNumeric16_8   TINYINT,
 cNumeric3_2    TINYINT);

select cId, cFloat, cDouble from testAltColPDD_n2 order by cId;
select cId, cDecimal38_18, cDecimal38_37, cDecimal16_8, cDecimal3_2 from testAltColPDD_n2
order by cId;
select cId, cNumeric38_18, cNumeric38_37, cNumeric16_8, cNumeric3_2 from testAltColPDD_n2
order by cId;

drop table if exists testAltColPDD_n2;
-- Parquet type with Dictionary encoding disabled: End
