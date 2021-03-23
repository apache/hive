DROP TABLE tmp_src1;

CREATE TABLE tmp_src1(
  `npp` string,
  `nsoc` string) stored as orc;

INSERT INTO tmp_src1 (npp,nsoc) VALUES ('1-1000CG61','7273111');
INSERT INTO tmp_src1 (npp,nsoc) VALUES ('1-1000CG61','7273112');
INSERT INTO tmp_src1 (npp,nsoc) VALUES ('1-1000EL62','7273221');
INSERT INTO tmp_src1 (npp,nsoc) VALUES ('1-1000EL62','7273222');
INSERT INTO tmp_src1 (npp,nsoc) VALUES ('1-1000OGH3','9392331');
INSERT INTO tmp_src1 (npp,nsoc) VALUES ('1-1000OGH3','9392334');
INSERT INTO tmp_src1 (npp,nsoc) VALUES ('1-1000Q7V4','7273444');
INSERT INTO tmp_src1 (npp,nsoc) VALUES ('1-1000Q7V4','7273441');
INSERT INTO tmp_src1 (npp,nsoc) VALUES ('1-1000WA85','7273554');
INSERT INTO tmp_src1 (npp,nsoc) VALUES ('1-1000WA85','7273555');

EXPLAIN CBO
SELECT `min_nsoc`
FROM
     (SELECT `npp`,
             MIN(`nsoc`) AS `min_nsoc`,
             COUNT(DISTINCT `nsoc`) AS `nb_nsoc`
      FROM tmp_src1
      GROUP BY `npp`) `a`
WHERE `nb_nsoc` > 0;

SELECT `min_nsoc`
FROM
    (SELECT `npp`,
            MIN(`nsoc`) AS `min_nsoc`,
            COUNT(DISTINCT `nsoc`) AS `nb_nsoc`
     FROM tmp_src1
     GROUP BY `npp`) `a`
WHERE `nb_nsoc` > 0;