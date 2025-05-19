--! qt:dataset:src1
--! qt:dataset:src
set hive.support.quoted.identifiers=none;
-- TOK_ALLCOLREF
explain cbo select * from (select a.key, a.* from (select * from src) a join (select * from src1) b on (a.key = b.key)) t;
explain select * from (select a.key, a.* from (select * from src) a join (select * from src1) b on (a.key = b.key)) t;
select * from (select a.key, a.* from (select * from src) a join (select * from src1) b on (a.key = b.key)) t;
-- DOT
explain cbo select * from (select a.key, a.`[k].*` from (select * from src) a join (select * from src1) b on (a.key = b.key)) t;
explain select * from (select a.key, a.`[k].*` from (select * from src) a join (select * from src1) b on (a.key = b.key)) t;
select * from (select a.key, a.`[k].*` from (select * from src) a join (select * from src1) b on (a.key = b.key)) t;
-- EXPRESSION
explain cbo select * from (select a.key, a.key from (select * from src) a join (select * from src1) b on (a.key = b.key)) t;
explain select * from (select a.key, a.key from (select * from src) a join (select * from src1) b on (a.key = b.key)) t;
select * from (select a.key, a.key from (select * from src) a join (select * from src1) b on (a.key = b.key)) t;

explain cbo select count(*) from (select key, key from src) subq;
explain select count(*) from (select key, key from src) subq;
select count(*) from (select key, key from src) subq;
