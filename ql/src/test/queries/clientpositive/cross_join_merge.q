--! qt:dataset:src1
--! qt:dataset:src
set hive.mapred.mode=nonstrict;

explain
select src1.key from src src1 join src src2 join src src3;

explain
select src1.key from src src1 join src src2 on src1.key=src2.key join src src3 on src1.key=src3.key;

explain
select src1.key from src src1 join src src2 join src src3 where src1.key=src2.key and src1.key=src3.key;

explain
select src1.key from src src1 join src src2 on 5 = src2.key join src src3 on src1.key=src3.key;

-- no merge
explain
select src1.key from src src1 left outer join src src2 join src src3;

explain
select src1.key from src src1 left outer join src src2 on src1.key=src2.key join src src3 on src1.key=src3.key;
