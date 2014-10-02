DROP TABLE avro_charvarchar_staging;
DROP TABLE avro_charvarchar;

CREATE TABLE avro_charvarchar_staging (
  cchar char(5),
  cvarchar varchar(10),
  m1 map<string, char(2)>,
  l1 array<string>,
  st1 struct<c1:int, c2:varchar(4)>
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':';

CREATE TABLE avro_charvarchar (
  cchar char(5),
  cvarchar varchar(10),
  m1 map<string, char(2)>,
  l1 array<string>,
  st1 struct<c1:int, c2:varchar(4)>
) STORED AS AVRO;

LOAD DATA LOCAL INPATH '../../data/files/avro_charvarchar.txt' OVERWRITE INTO TABLE avro_charvarchar_staging;

INSERT OVERWRITE TABLE avro_charvarchar SELECT * FROM avro_charvarchar_staging;

SELECT * FROM avro_charvarchar;
