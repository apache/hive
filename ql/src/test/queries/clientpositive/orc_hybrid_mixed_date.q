create table hybrid_table (d date)
stored as orc;

INSERT INTO hybrid_table VALUES
('2012-02-21'),
('2014-02-11'),
('1947-02-11'),
('8200-02-11'),
('1012-02-21'),
('1014-02-11'),
('0947-02-11'),
('0200-02-11');

select * from hybrid_table;

set orc.proleptic.gregorian.default=true;

select * from hybrid_table;

drop table hybrid_table;
