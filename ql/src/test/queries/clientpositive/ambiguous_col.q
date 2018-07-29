--! qt:dataset:src1
--! qt:dataset:src
set hive.support.quoted.identifiers=none;
-- TOK_ALLCOLREF
explain select * from (select a.key, a.* from (select * from src) a join (select * from src1) b on (a.key = b.key)) t;
select * from (select a.key, a.* from (select * from src) a join (select * from src1) b on (a.key = b.key)) t;
-- DOT
explain select * from (select a.key, a.`[k].*` from (select * from src) a join (select * from src1) b on (a.key = b.key)) t;
select * from (select a.key, a.`[k].*` from (select * from src) a join (select * from src1) b on (a.key = b.key)) t;
-- EXPRESSION
explain select * from (select a.key, a.key from (select * from src) a join (select * from src1) b on (a.key = b.key)) t;
select * from (select a.key, a.key from (select * from src) a join (select * from src1) b on (a.key = b.key)) t;

explain select count(*) from (select key, key from src) subq;
select count(*) from (select key, key from src) subq;
