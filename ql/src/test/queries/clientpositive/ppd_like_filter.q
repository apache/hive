CREATE TABLE test_tbl (a int) PARTITIONED BY (b string);

INSERT INTO test_tbl PARTITION(b="abc") VALUES (1);
INSERT INTO test_tbl PARTITION(b="d_\\%ae") VALUES (1);
INSERT INTO test_tbl PARTITION(b="af%") VALUES (1);

select * from test_tbl where b like 'a%';
select * from test_tbl where b like '%a%';
select * from test_tbl where b like '%a';
select * from test_tbl where b like '%a';

select * from test_tbl where b like 'a%c';
select * from test_tbl where b like '%a%c%';
select * from test_tbl where b like '%_b%';
select * from test_tbl where b like '%b_';

select * from test_tbl where b like '%c';
select * from test_tbl where b like '%c%';
select * from test_tbl where b like 'c%';
select * from test_tbl where b like '%\_%';

select * from test_tbl where b like '%\\%%';
select * from test_tbl where b like 'abc';
select * from test_tbl where b like '%';
select * from test_tbl where b like '_';

select * from test_tbl where b like '___';
select * from test_tbl where b like '%%%';
select * from test_tbl where b like '%\\\\%';



