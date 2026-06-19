drop table if exists predicate_fold_tb;

create table predicate_fold_tb(value int);
insert into predicate_fold_tb values(NULL), (1), (2), (3), (4), (5);

explain
SELECT * FROM predicate_fold_tb WHERE not(value IS NOT NULL AND value = 3);
SELECT * FROM predicate_fold_tb WHERE not(value IS NOT NULL AND value = 3);

explain
SELECT * FROM predicate_fold_tb WHERE not(value IS NOT NULL AND value >= 3);
SELECT * FROM predicate_fold_tb WHERE not(value IS NOT NULL AND value >= 3);

explain
SELECT * FROM predicate_fold_tb WHERE not(value IS NOT NULL AND value <= 3);
SELECT * FROM predicate_fold_tb WHERE not(value IS NOT NULL AND value <= 3);

explain
SELECT * FROM predicate_fold_tb WHERE not(value IS NOT NULL AND value > 3);
SELECT * FROM predicate_fold_tb WHERE not(value IS NOT NULL AND value > 3);

explain
SELECT * FROM predicate_fold_tb WHERE not(value IS NOT NULL AND value < 3);
SELECT * FROM predicate_fold_tb WHERE not(value IS NOT NULL AND value < 3);

explain
SELECT * FROM predicate_fold_tb WHERE not(value IS NOT NULL AND value <> 3);
SELECT * FROM predicate_fold_tb WHERE not(value IS NOT NULL AND value <> 3);

explain
SELECT * FROM predicate_fold_tb WHERE not(value IS NOT NULL AND value > 1 AND value <=3);
SELECT * FROM predicate_fold_tb WHERE not(value IS NOT NULL AND value > 1 AND value <=3);
