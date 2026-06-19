-- outer join is not qualified for pushing down of where to join condition
CREATE TABLE ltable (index int, la int, lk1 string, lk2 string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
CREATE TABLE rtable (ra int, rk1 string, rk2 string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

insert into ltable values (1, null, 'CD5415192314304', '00071'), (2, null, 'CD5415192225530', '00071');
insert into rtable values (1, 'CD5415192314304', '00071'), (45, 'CD5415192314304', '00072');

set hive.auto.convert.join=false;
EXPLAIN SELECT * FROM ltable l LEFT OUTER JOIN rtable r on (l.lk1 = r.rk1 AND l.lk2 = r.rk2) WHERE COALESCE(l.la,'EMPTY')=COALESCE(r.ra,'EMPTY');
SELECT * FROM ltable l LEFT OUTER JOIN rtable r on (l.lk1 = r.rk1 AND l.lk2 = r.rk2) WHERE COALESCE(l.la,'EMPTY')=COALESCE(r.ra,'EMPTY');

set hive.auto.convert.join=true;
EXPLAIN SELECT * FROM ltable l LEFT OUTER JOIN rtable r on (l.lk1 = r.rk1 AND l.lk2 = r.rk2) WHERE COALESCE(l.la,'EMPTY')=COALESCE(r.ra,'EMPTY');
SELECT * FROM ltable l LEFT OUTER JOIN rtable r on (l.lk1 = r.rk1 AND l.lk2 = r.rk2) WHERE COALESCE(l.la,'EMPTY')=COALESCE(r.ra,'EMPTY');