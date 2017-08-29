DROP TABLE IF EXISTS `dec`;

CREATE TABLE `dec` (
  name string,
  value decimal(8,4));

LOAD DATA LOCAL INPATH '../../data/files/dec.txt' into TABLE `dec`;

ANALYZE TABLE `dec` COMPUTE STATISTICS FOR COLUMNS value;
DESC FORMATTED `dec` value;

DROP TABLE IF EXISTS avro_dec;

CREATE TABLE avro_dec(
  name string,
  value decimal(5,2))
COMMENT 'just drop the schema right into the HQL'
STORED AS AVRO;

DESC avro_dec;

INSERT OVERWRITE TABLE avro_dec SELECT name, value FROM `dec`;

SELECT * FROM avro_dec;

DROP TABLE IF EXISTS avro_dec1;

CREATE TABLE avro_dec1(
  name string,
  value decimal(4,1))
COMMENT 'just drop the schema right into the HQL'
STORED AS AVRO;

DESC avro_dec1;

LOAD DATA LOCAL INPATH '../../data/files/dec.avro' INTO TABLE avro_dec1;

SELECT value FROM avro_dec1;

DROP TABLE `dec`;
DROP TABLE avro_dec;
DROP TABLE avro_dec1;
