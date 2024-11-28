drop table if exists json_serde3_1;
drop table if exists json_serde3_2;
drop table if exists json_serde3_3;
drop table if exists json_serde3_4;

create table json_serde3_1 (
    binarycolumn1 binary,
    binarycolumn2 binary,
    binarycolumn3 binary,
    binarycolumn4 binary,
    binarycolumn5 binary,
    binarycolumn6 binary
    )
  row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe';

LOAD DATA LOCAL INPATH '../../data/files/jsonserde.txt' into table json_serde3_1;
INSERT INTO TABLE json_serde3_1 VALUES (BINARY(CAST(-2 AS STRING)), BINARY(CAST(false AS STRING)), null, BINARY(CAST(true AS STRING)), BINARY(CAST(1.23e45 AS STRING)), "value");

select * from json_serde3_1;

create table json_serde3_2 (
    binarycolumn1 binary,
    binarycolumn2 binary,
    binarycolumn3 binary,
    binarycolumn4 binary,
    binarycolumn5 binary,
    binarycolumn6 binary)
  row format serde 'org.apache.hive.hcatalog.data.JsonSerDe';

LOAD DATA LOCAL INPATH '../../data/files/jsonserde.txt' into table json_serde3_2;
INSERT INTO TABLE json_serde3_2 VALUES (BINARY(CAST(-2 AS STRING)), BINARY(CAST(false AS STRING)), null, BINARY(CAST(true AS STRING)), BINARY(CAST(1.23e45 AS STRING)), "value");

select * from json_serde3_2;

create table json_serde3_3 (
    booleancaseinsensitive boolean,
    booleanstring boolean,
    booleanboolean boolean,
    stringfalse boolean,
    somestring boolean,
    booleannull boolean,
    booleannumfalse boolean,
    booleannumtrue boolean)
  row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe';

LOAD DATA LOCAL INPATH '../../data/files/jsonserde.txt' into table json_serde3_3;
INSERT INTO TABLE json_serde3_3 VALUES ("TrUE", "true", true, "FaLSE", "somestringhere", null, 0, -1);

select * from json_serde3_3;

create table json_serde3_4 (
    booleancaseinsensitive boolean,
    booleanstring boolean,
    booleanboolean boolean,
    stringfalse boolean,
    somestring boolean,
    booleannull boolean,
    booleannumfalse boolean,
    booleannumtrue boolean)
  row format serde 'org.apache.hive.hcatalog.data.JsonSerDe';

LOAD DATA LOCAL INPATH '../../data/files/jsonserde.txt' into table json_serde3_4;
INSERT INTO TABLE json_serde3_4 VALUES ("TrUE", "true", true, "FaLSE", "somestringhere", null, 0, -1);

select * from json_serde3_4;

drop table json_serde3_1;
drop table json_serde3_2;
drop table json_serde3_3;
drop table json_serde3_4;