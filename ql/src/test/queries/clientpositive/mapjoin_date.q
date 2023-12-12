set hive.auto.convert.join=true;

CREATE TABLE person (fname string, birthDate date);
INSERT INTO person VALUES ('Victor', '2023-11-27'), ('Alexandre', '2023-11-28');

EXPLAIN VECTORIZATION DETAIL SELECT * FROM person p1 INNER JOIN person p2 ON p1.birthDate=p2.birthDate;

SELECT * FROM person p1 INNER JOIN person p2 ON p1.birthDate=p2.birthDate;
