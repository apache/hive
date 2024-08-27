-- verify that Hive fails to read a union type column from avro file with null union data if AVRO schema definition is not nullable

DROP TABLE IF EXISTS union_nullable_test_text;

CREATE TABLE union_nullable_test_text (id int, value uniontype<int,double>) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY ':' STORED AS textfile;

LOAD DATA LOCAL INPATH '../../data/files/union_nullable.txt' INTO TABLE union_nullable_test_text;

DROP TABLE IF EXISTS union_non_nullable_test_avro;

CREATE TABLE union_non_nullable_test_avro STORED AS avro tblproperties('avro.schema.literal'='{"name":"nullable", "type":"record", "fields":[{"name":"id", "type":"int"}, {"name":"value", "type":["int", "double"]}]}');

INSERT OVERWRITE TABLE union_non_nullable_test_avro SELECT * FROM union_nullable_test_text;

SELECT * FROM union_non_nullable_test_avro;

DROP TABLE union_nullable_test_text;
DROP TABLE union_non_nullable_test_avro;

