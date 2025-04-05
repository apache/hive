set hive.cbo.enable=false;

CREATE TABLE students (name VARCHAR(64), age INT, gpa DECIMAL(3,2));

INSERT INTO TABLE students VALUES ('s1', 1, 1.23);

INSERT INTO TABLE students(name, age, gpa) VALUES('s2', 2, 2.34);

SELECT * FROM students;

-- Partitioned table
CREATE TABLE people (id int, name string, age int) PARTITIONED BY (country string);

INSERT INTO people PARTITION (country="Cuba") VALUES (1, "abc", 23);

INSERT INTO people PARTITION (country="Denmark") (id, name, age)  VALUES (2, "bcd", 34);

INSERT INTO people PARTITION (country) VALUES (3, "cde", 45, "Egypt");

INSERT INTO people PARTITION (country) (id, name, age, country)  VALUES (4, "def", 56, "Finland");

INSERT INTO people VALUES (5, "efg", 67, "Ghana");

INSERT INTO people (id, name, age, country) VALUES (6, "fgh", 78, "Hungary");

SELECT * FROM people;