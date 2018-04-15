--! qt:dataset:src
-- COMMENT
select key from src limit 1;

/* comment comment */
select key from src limit 1;

select /*comment*/ key from src limit 1;

select /*comment*/ key from /* comment */ src /* comment */ limit 1;

select /**/ key /* */ from src limit 1;

/*

*/
select /*
*/ key from src limit 1;
set hive.auto.convert.join=true;
select /*+ MAPJOIN(a) */ count(*) from src a join src b on a.key = b.key where a.key > 0;

explain extended select /*+ MAPJOIN(a) */ count(*) from src a join src b on a.key = b.key where a.key > 0;

reset hive.auto.convert.join;
