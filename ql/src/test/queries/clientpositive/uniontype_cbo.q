CREATE TABLE utable (cu UNIONTYPE<INTEGER, STRING>);
DESCRIBE utable;

EXPLAIN CBO 
INSERT INTO utable values
(create_union(0, 10, 'ten')),
(create_union(1, 10, 'ten'));
INSERT INTO utable values
(create_union(0, 10, 'ten')),
(create_union(1, 10, 'ten'));

EXPLAIN CBO
SELECT cu FROM utable;
SELECT cu FROM utable;

EXPLAIN CBO
SELECT extract_union(cu) FROM utable;
SELECT extract_union(cu) FROM utable;

CREATE TABLE author (id INT, fname STRING, age INT);
INSERT INTO author VALUES (0, 'Victor' , '37'), (1, 'Alexander' , '44');

EXPLAIN CBO
CREATE TABLE uauthor AS SELECT create_union(id % 2, age, fname) as u_age_name FROM author;
CREATE TABLE uauthor AS SELECT create_union(id % 2, age, fname) as u_age_name FROM author;

DESCRIBE uauthor;

EXPLAIN CBO
SELECT u_age_name FROM uauthor;
SELECT u_age_name FROM uauthor;
