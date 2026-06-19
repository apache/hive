--! qt:dataset:tpch_0_001.customer
--! qt:dataset:tpch_0_001.lineitem
--! qt:dataset:tpch_0_001.nation
--! qt:dataset:tpch_0_001.orders
--! qt:dataset:tpch_0_001.part
--! qt:dataset:tpch_0_001.partsupp
--! qt:dataset:tpch_0_001.region
--! qt:dataset:tpch_0_001.supplier

USE tpch_0_001;

SELECT COUNT(1) FROM customer;
SELECT COUNT(1) FROM lineitem;
SELECT COUNT(1) FROM nation;
SELECT COUNT(1) FROM orders;
SELECT COUNT(1) FROM part;
SELECT COUNT(1) FROM partsupp;
SELECT COUNT(1) FROM region;
SELECT COUNT(1) FROM supplier;