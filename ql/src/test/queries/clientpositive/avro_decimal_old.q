DROP TABLE IF EXISTS avro_dec_old;

CREATE TABLE `avro_dec_old`(
  `name` string COMMENT 'from deserializer',
  `value` decimal(4,1) COMMENT 'from deserializer')
STORED AS AVRO;

DESC avro_dec_old;

LOAD DATA LOCAL INPATH '../../data/files/dec_old.avro' into TABLE avro_dec_old;

select value from avro_dec_old;

DROP TABLE avro_dec_old;
