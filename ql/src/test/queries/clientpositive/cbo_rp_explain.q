set hive.cbo.returnpath.hiveop=true;

CREATE TABLE author (id INT, fname STRING, lname STRING, birth DATE);
CREATE TABLE book (id INT, title STRING, author INT);

EXPLAIN CBO
SELECT lname, MAX(birth) FROM author GROUP BY lname;

EXPLAIN CBO
SELECT author.lname, book.title
FROM author 
INNER JOIN book ON author.id=book.author
WHERE author.fname = 'Victor';
