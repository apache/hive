set hive.mapred.mode=nonstrict;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir};
dfs -copyFromLocal ../../data/files/header_footer_table_4  ${system:test.tmp.dir}/header_footer_table_4;

CREATE TABLE numbrs (numbr int);
INSERT INTO numbrs VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12), (NULL);
CREATE EXTERNAL TABLE header_footer_table_4 (header_int int, header_name string, header_choice varchar(10)) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '${system:test.tmp.dir}/header_footer_table_4' tblproperties ("skip.header.line.count"="1", "skip.footer.line.count"="2");

SELECT * FROM header_footer_table_4;

SELECT * FROM header_footer_table_4 ORDER BY header_int LIMIT 8;

-- should return nothing as title is correctly skipped
SELECT * FROM header_footer_table_4 WHERE header_choice = 'header_choice';
SELECT * FROM header_footer_table_4 WHERE header_choice = 'monthly';
SELECT COUNT(*) FROM header_footer_table_4;

-- shpuld return nothing
SELECT * FROM header_footer_table_4 WHERE header_choice is NULL;


SELECT AVG(header_int) FROM header_footer_table_4 GROUP BY header_choice;

-- should not include any header and footer
SELECT * FROM header_footer_table_4 A, header_footer_table_4 B ORDER BY A.header_int, B.header_int;

-- no join variant should include header or footer
SELECT header_name, header_int FROM header_footer_table_4 LEFT JOIN numbrs ON numbr = header_int;
SELECT header_name, header_int FROM header_footer_table_4 RIGHT JOIN numbrs ON numbr = header_int;
SELECT header_name, header_int FROM header_footer_table_4 INNER JOIN numbrs ON numbr = header_int;
SELECT header_name, header_int FROM header_footer_table_4 FULL JOIN numbrs ON numbr = header_int;


SELECT header_choice, SUM(header_int) FROM header_footer_table_4 GROUP BY header_choice;


SELECT DISTINCT header_choice, SUM(header_int) FROM header_footer_table_4 GROUP BY header_choice;


SELECT header_name, header_choice FROM header_footer_table_4 ORDER BY header_int LIMIT 8;

-- create table from existing text file should not feed header or footer to the new table
CREATE TABLE transition (title VARCHAR(10), name VARCHAR(10)) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE LOCATION '${system:test.tmp.dir}/header_footer_table_transition';
INSERT INTO transition SELECT header_choice, header_name FROM header_footer_table_4;
SELECT * FROM transition A, transition B ORDER BY A.title, A.name, B.title, B.name;

CREATE TABLE transition2 (header_choice VARCHAR(10), sum_header_int int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE LOCATION '${system:test.tmp.dir}/test/header_footer_table_transition2';
INSERT INTO transition2 SELECT header_choice, SUM(header_int) FROM header_footer_table_4 GROUP BY header_choice;
SELECT * FROM transition2 A, transition2 B ORDER BY A.sum_header_int, A.header_choice, B.sum_header_int, B.header_choice;

DROP TABLE transition;
DROP TABLE transition2;

-- turn off fetch conversion. This disables the additional header/footer handling logic in fetch operator.
set hive.fetch.task.conversion=none;

SELECT * FROM header_footer_table_4;

SELECT * FROM header_footer_table_4 ORDER BY header_int LIMIT 8;

-- should return nothing as title is correctly skipped
SELECT * FROM header_footer_table_4 WHERE header_choice = 'header_choice';
SELECT * FROM header_footer_table_4 WHERE header_choice = 'monthly';
SELECT COUNT(*) FROM header_footer_table_4;

-- shpuld return nothing
SELECT * FROM header_footer_table_4 WHERE header_choice is NULL;


SELECT AVG(header_int) FROM header_footer_table_4 GROUP BY header_choice;

-- should not include any header and footer
SELECT * FROM header_footer_table_4 A, header_footer_table_4 B ORDER BY A.header_int, B.header_int;

-- no join variant should include header or footer
SELECT header_name, header_int FROM header_footer_table_4 LEFT JOIN numbrs ON numbr = header_int;
SELECT header_name, header_int FROM header_footer_table_4 RIGHT JOIN numbrs ON numbr = header_int;
SELECT header_name, header_int FROM header_footer_table_4 INNER JOIN numbrs ON numbr = header_int;
SELECT header_name, header_int FROM header_footer_table_4 FULL JOIN numbrs ON numbr = header_int;


SELECT header_choice, SUM(header_int) FROM header_footer_table_4 GROUP BY header_choice;


SELECT DISTINCT header_choice, SUM(header_int) FROM header_footer_table_4 GROUP BY header_choice;


SELECT header_name, header_choice FROM header_footer_table_4 ORDER BY header_int LIMIT 8;

-- create table from existing text file should not feed header or footer to the new table
CREATE TABLE transition (title VARCHAR(10), name VARCHAR(10)) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE LOCATION '${system:test.tmp.dir}/header_footer_table_transition';
INSERT INTO transition SELECT header_choice, header_name FROM header_footer_table_4;
SELECT * FROM transition A, transition B ORDER BY A.title, A.name, B.title, B.name;

CREATE TABLE transition2 (header_choice VARCHAR(10), sum_header_int int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE LOCATION '${system:test.tmp.dir}/test/header_footer_table_transition2';
INSERT INTO transition2 SELECT header_choice, SUM(header_int) FROM header_footer_table_4 GROUP BY header_choice;
SELECT * FROM transition2 A, transition2 B ORDER BY A.sum_header_int, A.header_choice, B.sum_header_int, B.header_choice;
