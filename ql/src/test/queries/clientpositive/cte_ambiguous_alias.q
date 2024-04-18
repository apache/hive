CREATE TABLE tbl2(key int, value string);

-- set hive.query.lifetime.hooks=;
explain cbo
select * from 
  (select * from tbl2 a where key < 6) subq1 
    join
  (select * from tbl2 a where key < 6) subq2
  on (subq1.key = subq2.key)
    join
  (select * from tbl2 a where key < 6) subq3
  on (subq1.key = subq3.key);