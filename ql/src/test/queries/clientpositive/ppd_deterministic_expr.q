set hive.auto.convert.join=false;
set hive.optimize.index.filter=true;
set hive.cbo.enable=false;

CREATE TABLE `testb`(
   `cola` string COMMENT '',
   `colb` string COMMENT '',
   `colc` string COMMENT '')
PARTITIONED BY (
   `part1` string,
   `part2` string,
   `part3` string)

STORED AS AVRO;

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

insert into testB partition (part1='CA', part2='ABC', part3='300')
values ('600', '700', 'abc'), ('601', '701', 'abcd');

insert into testB partition (part1='CA', part2='ABC', part3='400')
values ( '600', '700', 'abc'), ( '601', '701', 'abcd');

insert into testB partition (part1='UK', part2='PQR', part3='500')
values ('600', '700', 'abc'), ('601', '701', 'abcd');

insert into testB partition (part1='US', part2='DEF', part3='200')
values ( '600', '700', 'abc'), ('601', '701', 'abcd');

insert into testB partition (part1='US', part2='PQR', part3='123')
values ( '600', '700', 'abc'), ('601', '701', 'abcd');

-- views with deterministic functions
create view viewDeterministicUDFA partitioned on (vpart1, vpart2, vpart3) as select
 cast(col1 as decimal(38,18)) as vcol1,
 cast(col2 as decimal(38,18)) as vcol2,
 cast(col3 as decimal(38,18)) as vcol3,
 cast(col4 as decimal(38,18)) as vcol4,
 cast(col5 as char(10)) as vcol5,
 cast(part1 as char(2)) as vpart1,
 cast(part2 as char(3)) as vpart2,
 cast(part3 as char(3)) as vpart3
 from testa
where part1 in ('US', 'CA');

create view viewDeterministicUDFB partitioned on (vpart1, vpart2, vpart3) as select
 cast(cola as decimal(38,18)) as vcolA,
 cast(colb as decimal(38,18)) as vcolB,
 cast(colc as char(10)) as vcolC,
 cast(part1 as char(2)) as vpart1,
 cast(part2 as char(3)) as vpart2,
 cast(part3 as char(3)) as vpart3
 from testb
where part1 in ('US', 'CA');

-- views without function reference
create view viewNoUDFA partitioned on (part1, part2, part3) as select
 cast(col1 as decimal(38,18)) as vcol1,
 cast(col2 as decimal(38,18)) as vcol2,
 cast(col3 as decimal(38,18)) as vcol3,
 cast(col4 as decimal(38,18)) as vcol4,
 cast(col5 as char(10)) as vcol5,
 part1,
 part2,
 part3
 from testa
where part1 in ('US', 'CA');

create view viewNoUDFB partitioned on (part1, part2, part3) as select
 cast(cola as decimal(38,18)) as vcolA,
 cast(colb as decimal(38,18)) as vcolB,
 cast(colc as char(10)) as vcolC,
 part1,
 part2,
 part3
 from testb
where part1 in ('US', 'CA');

-- query referencing deterministic functions
explain
select vcol1, vcol2, vcol3, vcola, vcolb
from viewDeterministicUDFA a inner join viewDeterministicUDFB b
on a.vpart1 = b.vpart1
and a.vpart2 = b.vpart2
and a.vpart3 = b.vpart3
and a.vpart1 = 'US'
and a.vpart2 = 'DEF'
and a.vpart3 = '200';

select vcol1, vcol2, vcol3, vcola, vcolb
from viewDeterministicUDFA a inner join viewDeterministicUDFB b
on a.vpart1 = b.vpart1
and a.vpart2 = b.vpart2
and a.vpart3 = b.vpart3
and a.vpart1 = 'US'
and a.vpart2 = 'DEF'
and a.vpart3 = '200';

-- query with views referencing no udfs
explain
select vcol1, vcol2, vcol3, vcola, vcolb
from viewNoUDFA a inner join viewNoUDFB b
on a.part1 = b.part1
and a.part2 = b.part2
and a.part3 = b.part3
and a.part1 = 'US'
and a.part2 = 'DEF'
and a.part3 = '200';

select vcol1, vcol2, vcol3, vcola, vcolb
from viewNoUDFA a inner join viewNoUDFB b
on a.part1 = b.part1
and a.part2 = b.part2
and a.part3 = b.part3
and a.part1 = 'US'
and a.part2 = 'DEF'
and a.part3 = '200';
