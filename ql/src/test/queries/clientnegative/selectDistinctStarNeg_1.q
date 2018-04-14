--! qt:dataset:src1
--! qt:dataset:src
-- Duplicate column name: key

drop view if exists v;
create view v as select distinct * from src join src1 on src.key=src1.key;