set hive.parquet.date.proleptic.gregorian=true;

create table proleptic_table (d date)
stored as parquet;

INSERT INTO proleptic_table VALUES
('2012-02-21'),
('2014-02-11'),
('1947-02-11'),
('8200-02-11'),
('1012-02-21'),
('1014-02-11'),
('0947-02-11'),
('0200-02-11');

select * from proleptic_table;

set hive.parquet.date.proleptic.gregorian.default=true;

select * from proleptic_table;

drop table proleptic_table;
