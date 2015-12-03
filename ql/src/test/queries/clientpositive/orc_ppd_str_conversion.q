set hive.cbo.enable=false;

create table orc_test( col1 varchar(15), col2 char(10)) stored as orc;
create table text_test( col1 varchar(15), col2 char(10));

insert into orc_test values ('val1', '1');
insert overwrite table text_test select * from orc_test;

explain select * from text_test where col2='1';
select * from text_test where col2='1';

set hive.optimize.index.filter=false;
select * from orc_test where col2='1';

set hive.optimize.index.filter=true;
select * from orc_test where col2='1';

