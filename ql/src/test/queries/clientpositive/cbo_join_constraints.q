CREATE TABLE `store_sales` (`ss_item_sk` bigint);
CREATE TABLE `household_demographics` (`hd_demo_sk` bigint);
CREATE TABLE `item` (`i_item_sk` bigint);
ALTER TABLE `store_sales` ADD CONSTRAINT `pk_ss` PRIMARY KEY (`ss_item_sk`) DISABLE NOVALIDATE RELY;
ALTER TABLE `item` ADD CONSTRAINT `pk_i` PRIMARY KEY (`i_item_sk`) DISABLE NOVALIDATE RELY;
ALTER TABLE `store_sales` ADD CONSTRAINT `ss_i` FOREIGN KEY (`ss_item_sk`) REFERENCES `item`(`i_item_sk`) DISABLE NOVALIDATE RELY;

EXPLAIN CBO
SELECT i_item_sk
FROM store_sales, household_demographics, item
WHERE ss_item_sk = i_item_sk;
