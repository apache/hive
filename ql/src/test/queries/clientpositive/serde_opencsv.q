EXPLAIN
CREATE TABLE serde_opencsv(
                          words STRING,
                          int1 INT,
                          tinyint1 TINYINT,
                          smallint1 SMALLINT,
                          bigint1 BIGINT,
                          boolean1 BOOLEAN,
                          float1 FLOAT,
                          double1 DOUBLE)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES(
  "separatorChar" = ",",
  "quoteChar"     = "\'",
  "escapeChar"    = "\\"
) stored as textfile;

CREATE TABLE serde_opencsv(
                          words STRING,
                          int1 INT,
                          tinyint1 TINYINT,
                          smallint1 SMALLINT,
                          bigint1 BIGINT,
                          boolean1 BOOLEAN,
                          float1 FLOAT,
                          double1 DOUBLE)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES(
  "separatorChar" = ",",
  "quoteChar"     = "\'",
  "escapeChar"    = "\\"
) stored as textfile;

LOAD DATA LOCAL INPATH "../../data/files/opencsv-data.txt" INTO TABLE serde_opencsv;

SELECT count(*) FROM serde_opencsv;
