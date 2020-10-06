--! qt:dataset:impala_dataset

explain select cast (3 as string);
explain select cast ('3' as double);
--CDPD-17024, add more cast tests
--DWX-5680, test double times a decimal
explain SELECT 0.10000000000000001 * lat FROM impala_airports;
--DWX-5680, test decimal times a decimal
explain SELECT 0.10000000000000001 * c_acctbal FROM impala_tpch_customer;
