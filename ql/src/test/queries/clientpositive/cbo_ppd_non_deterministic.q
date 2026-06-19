CREATE TABLE `testa`(
   `col1` string COMMENT '',
   `col2` string COMMENT '',
   `col3` string COMMENT '',
   `col4` string COMMENT '',
   `col5` string COMMENT '')
PARTITIONED BY (
   `part1` string,
   `part2` string,
   `part3` string)
STORED AS AVRO;

insert into testA partition (part1='US', part2='ABC', part3='123')
values ('12.34', '100', '200', '300', 'abc'),
('12.341', '1001', '2001', '3001', 'abcd');

insert into testA partition (part1='UK', part2='DEF', part3='123')
values ('12.34', '100', '200', '300', 'abc'),
('12.341', '1001', '2001', '3001', 'abcd');

insert into testA partition (part1='US', part2='DEF', part3='200')
values ('12.34', '100', '200', '300', 'abc'),
('12.341', '1001', '2001', '3001', 'abcd');

insert into testA partition (part1='CA', part2='ABC', part3='300')
values ('12.34', '100', '200', '300', 'abc'),
('12.341', '1001', '2001', '3001', 'abcd');

set hive.cbo.enable=true;
SET hive.vectorized.execution.enabled=false;

explain select * from (
select part1,randum123
from (SELECT *, cast(rand() as double) AS randum123 FROM testA where part1='CA' and part2 = 'ABC') a
where randum123 <= 0.5) s where s.randum123 > 0.25 limit 20;

SET hive.vectorized.execution.enabled=true;

explain select * from (
select part1,randum123
from (SELECT *, cast(rand() as double) AS randum123 FROM testA where part1='CA' and part2 = 'ABC') a
where randum123 <= 0.5) s where s.randum123 > 0.25 limit 20;
