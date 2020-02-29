create table hybrid_table (d timestamp)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ',' MAP KEYS TERMINATED BY ':'
stored as avro;

INSERT INTO hybrid_table VALUES
('2012-02-21 07:08:09.123'),
('2014-02-11 07:08:09.123'),
('1947-02-11 07:08:09.123'),
('8200-02-11 07:08:09.123'),
('1012-02-21 07:15:11.123'),
('1014-02-11 07:15:11.123'),
('0947-02-11 07:15:11.123'),
('0200-02-11 07:15:11.123');

select * from hybrid_table;

set hive.avro.proleptic.gregorian.default=true;

select * from hybrid_table;

drop table hybrid_table;
