set hive.strict.checks.type.safety=false;

CREATE TABLE batch1_19671_fin(
    pts char(15),
    CoNameOrCIK char(60)
);

CREATE TABLE dimCompany(
    sk_CompanyId bigint,
    name char(60),
    companyId bigint
);

CREATE TABLE  Financial (
  SK_CompanyID BIGINT,
  FI_YEAR NUMERIC(4) ,
  FI_QTR char(10) ,
  FI_QTR_START_DATE DATE,
  cId        TINYINT,
  cTimeStamp TIMESTAMP,
  cDecimal   DECIMAL(38,18),
  cDouble    DOUBLE,
  cFloat     FLOAT,
  cBigInt    BIGINT,
  cInt       INT,
  cSmallInt  SMALLINT,
  cTinyint   TINYINT,
  cBoolean   BOOLEAN
);

CREATE TABLE  FinancialPart (
  SK_CompanyID BIGINT,
  FI_YEAR NUMERIC(4) ,
  FI_QTR char(10) ,
  FI_QTR_START_DATE DATE,
  cId        TINYINT,
  cTimeStamp TIMESTAMP,
  cDecimal   DECIMAL(38,18),
  cDouble    DOUBLE,
  cFloat     FLOAT,
  cBigInt    BIGINT,
  cInt       INT,
  cSmallInt  SMALLINT,
  cTinyint   TINYINT,
  cBoolean   BOOLEAN
)
PARTITIONED BY (country string);

-- This insert will add stats with null bit-vector as there is no records matching
INSERT INTO FinancialPart partition (country='country1') (
  SK_CompanyID,
  FI_YEAR,
  FI_QTR,
  FI_QTR_START_DATE,
  cId,
  cTimeStamp,
  cDecimal,
  cDouble,
  cFloat,
  cBigInt,
  cInt,
  cSmallInt,
  cTinyint,
  cBoolean
)
SELECT
   DimCompany.SK_CompanyID,
   2021,
   'third',
   to_date('2021-11-21'),
   1,
   '2017-11-07 09:02:49.999999999',
   12345678901234567890.123456789012345678,
   1.79e308,
   3.4e38,
   1234567890123456789,
   1234567890,
   12345,
   123,
   TRUE
FROM
  batch1_19671_fin finwire_fin
  JOIN DimCompany ON (
    finwire_fin.CoNameOrCIK = DimCompany.CompanyID )
UNION
SELECT
   DimCompany.SK_CompanyID,
   2021,
   'first',
   to_date('2021-01-21'),
   1,
   '2017-11-07 09:02:49.999999999',
   12345678901234567890.123456789012345678,
   1.79e308,
   3.4e38,
   1234567890123456789,
   1234567890,
   12345,
   123,
   TRUE
FROM
  batch1_19671_fin finwire_fin
  JOIN DimCompany ON (
    finwire_fin.CoNameOrCIK = DimCompany.Name);

desc formatted FinancialPart partition(country='country1') SK_CompanyID;

-- This insert will try to merge the existing bit vector which is null
INSERT INTO FinancialPart partition (country) values
 (
  1099511627778,
  1967,
  'second',
  to_date('1967-04-01'),
  1,
   '2017-11-07 09:02:49.999999999',
   12345678901234567890.123456789012345678,
   1.79e308,
   3.4e38,
   1234567890123456789,
   1234567890,
   12345,
   123,
   TRUE,
   'country3'
);

desc formatted FinancialPart partition(country='country1') SK_CompanyID;
desc formatted FinancialPart partition(country='country3') SK_CompanyID;

-- This insert will add stats with null bit-vector as there is no records matching
INSERT INTO Financial (
  SK_CompanyID,
  FI_YEAR,
  FI_QTR,
  FI_QTR_START_DATE,
  cId,
  cTimeStamp,
  cDecimal,
  cDouble,
  cFloat,
  cBigInt,
  cInt,
  cSmallInt,
  cTinyint,
  cBoolean
)
SELECT
   DimCompany.SK_CompanyID,
   2021,
   'third',
   to_date('2021-11-21'),
   1,
   '2017-11-07 09:02:49.999999999',
   12345678901234567890.123456789012345678,
   1.79e308,
   3.4e38,
   1234567890123456789,
   1234567890,
   12345,
   123,
   TRUE
FROM
  batch1_19671_fin finwire_fin
  JOIN DimCompany ON (
    finwire_fin.CoNameOrCIK = DimCompany.CompanyID )
UNION
SELECT
   DimCompany.SK_CompanyID,
   2021,
   'first',
   to_date('2021-01-21'),
   1,
   '2017-11-07 09:02:49.999999999',
   12345678901234567890.123456789012345678,
   1.79e308,
   3.4e38,
   1234567890123456789,
   1234567890,
   12345,
   123,
   TRUE
FROM
  batch1_19671_fin finwire_fin
  JOIN DimCompany ON (
    finwire_fin.CoNameOrCIK = DimCompany.Name);

desc formatted Financial FI_QTR;

-- This insert will try to merge the existing bit vector which is null
INSERT INTO Financial values
 (
  1099511627778,
  1967,
  'second',
  to_date('1967-04-01'),
  1,
   '2017-11-07 09:02:49.999999999',
   12345678901234567890.123456789012345678,
   1.79e308,
   3.4e38,
   1234567890123456789,
   1234567890,
   12345,
   123,
   TRUE
);

desc formatted Financial FI_QTR;

