--! qt:dataset:impala_dataset

explain select cast (3 as string);
explain select cast ('3' as double);
--CDPD-17024, add more cast tests
