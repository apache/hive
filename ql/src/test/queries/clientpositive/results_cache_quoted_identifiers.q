--! qt:dataset:src

create table quoted1 (
  `_c1` int,
  `int` int,
  `col 3` string,
  `col``4` string
) stored as textfile;

insert into quoted1 select key, key, value, value from src;

set hive.query.results.cache.enabled=true;
set hive.query.results.cache.nontransactional.tables.enabled=true;

explain
select max(`_c1`), max(`int`), max(`col 3`), max(`col``4`) from quoted1;
select max(`_c1`), max(`int`), max(`col 3`), max(`col``4`) from quoted1;

set test.comment="Cache should be used for this query";
set test.comment;
explain
select max(`_c1`), max(`int`), max(`col 3`), max(`col``4`) from quoted1;
select max(`_c1`), max(`int`), max(`col 3`), max(`col``4`) from quoted1;
