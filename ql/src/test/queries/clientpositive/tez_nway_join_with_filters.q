set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.auto.convert.join=true;

create temporary table foo (id bigint, code string) stored as orc;
create temporary table bar (id bigint, code string) stored as orc;
create temporary table baz (id bigint) stored as orc;

-- SORT_QUERY_RESULTS

INSERT INTO foo values
  (29999000052073, '01'),
  (29999000052107, '01'),
  (29999000052111, '01'),
  (29999000052112, '01'),
  (29999000052113, '01'),
  (29999000052114, '01'),
  (29999000052071, '01A'),
  (29999000052072, '01A'),
  (29999000052116, '01A'),
  (29999000052117, '01A'),
  (29999000052118, '01A'),
  (29999000052119, '01A'),
  (29999000052120, '01A'),
  (29999000052076, '06'),
  (29999000052074, '06A'),
  (29999000052075, '06A');
INSERT INTO bar values
  (29999000052071, '01'),
  (29999000052072, '01'),
  (29999000052073, '01'),
  (29999000052116, '01'),
  (29999000052117, '01'),
  (29999000052071, '01A'),
  (29999000052072, '01A'),
  (29999000052073, '01A'),
  (29999000052116, '01AS'),
  (29999000052117, '01AS'),
  (29999000052071, '01B'),
  (29999000052072, '01B'),
  (29999000052073, '01B'),
  (29999000052116, '01BS'),
  (29999000052117, '01BS'),
  (29999000052071, '01C'),
  (29999000052072, '01C'),
  (29999000052073, '01C7'),
  (29999000052116, '01CS'),
  (29999000052117, '01CS'),
  (29999000052071, '01D'),
  (29999000052072, '01D'),
  (29999000052073, '01D'),
  (29999000052116, '01DS'),
  (29999000052117, '01DS');
INSERT INTO baz values
  (29999000052071),
  (29999000052072),
  (29999000052073),
  (29999000052074),
  (29999000052075),
  (29999000052076),
  (29999000052107),
  (29999000052111),
  (29999000052112),
  (29999000052113),
  (29999000052114),
  (29999000052116),
  (29999000052117),
  (29999000052118),
  (29999000052119),
  (29999000052120);

set hive.merge.nway.joins=true;
explain select a.id, b.code, c.id from foo a left outer join bar b on a.id = b.id and (a.code = '01AS' or b.code = '01BS') left outer join baz c on a.id = c.id;

set hive.merge.nway.joins=false;
explain select a.id, b.code, c.id from foo a left outer join bar b on a.id = b.id and (a.code = '01AS' or b.code = '01BS') left outer join baz c on a.id = c.id;

set hive.merge.nway.joins=true;
select a.id, b.code, c.id from foo a left outer join bar b on a.id = b.id and (a.code = '01AS' or b.code = '01BS') left outer join baz c on a.id = c.id;

set hive.merge.nway.joins=false;
select a.id, b.code, c.id from foo a left outer join bar b on a.id = b.id and (a.code = '01AS' or b.code = '01BS') left outer join baz c on a.id = c.id;

set hive.merge.nway.joins=true;
explain select a.id, b.code, c.id from foo a inner join bar b on a.id = b.id and (a.code = '01AS' or b.code = '01BS') left outer join baz c on a.id = c.id;

set hive.merge.nway.joins=false;
explain select a.id, b.code, c.id from foo a inner join bar b on a.id = b.id and (a.code = '01AS' or b.code = '01BS') left outer join baz c on a.id = c.id;

set hive.merge.nway.joins=true;
select a.id, b.code, c.id from foo a inner join bar b on a.id = b.id and (a.code = '01AS' or b.code = '01BS') left outer join baz c on a.id = c.id;

set hive.merge.nway.joins=false;
select a.id, b.code, c.id from foo a inner join bar b on a.id = b.id and (a.code = '01AS' or b.code = '01BS') left outer join baz c on a.id = c.id;
