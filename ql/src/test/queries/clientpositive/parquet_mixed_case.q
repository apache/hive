DROP TABLE parquet_mixed_case;

CREATE TABLE parquet_mixed_case (
  lowerCase string,
  UPPERcase string,
  stats bigint,
  moreuppercase string,
  MORELOWERCASE string
) STORED AS PARQUET;

LOAD DATA LOCAL INPATH '../../data/files/parquet_mixed_case' OVERWRITE INTO TABLE parquet_mixed_case;

SELECT lowercase, "|", uppercase, "|", stats, "|", moreuppercase, "|", morelowercase FROM parquet_mixed_case;
