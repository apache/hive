CREATE TABLE `store_sales` (`ss_item_sk` bigint, `ss_hdemo_sk` bigint);
CREATE TABLE `household_demographics` (`hd_demo_sk` bigint, `hd_income_band_sk` bigint);
CREATE TABLE `item` (`i_item_sk` bigint, `i_product_name` string);
CREATE TABLE `income_band`(`ib_income_band_sk` bigint);
CREATE TABLE `customer`(`c_current_hdemo_sk` bigint);
ALTER TABLE `store_sales` ADD CONSTRAINT `pk_ss` PRIMARY KEY (`ss_item_sk`) DISABLE NOVALIDATE RELY;
ALTER TABLE `item` ADD CONSTRAINT `pk_i` PRIMARY KEY (`i_item_sk`) DISABLE NOVALIDATE RELY;
ALTER TABLE `household_demographics` ADD CONSTRAINT `pk_hd` PRIMARY KEY (`hd_demo_sk`) DISABLE NOVALIDATE RELY;
ALTER TABLE `income_band` ADD CONSTRAINT `pk_ib` PRIMARY KEY (`ib_income_band_sk`) DISABLE NOVALIDATE RELY;
ALTER TABLE `store_sales` ADD CONSTRAINT `ss_i` FOREIGN KEY (`ss_item_sk`) REFERENCES `item`(`i_item_sk`) DISABLE NOVALIDATE RELY;
ALTER TABLE `household_demographics` ADD CONSTRAINT `hd_ib` FOREIGN KEY (`hd_income_band_sk`) REFERENCES `income_band`(`ib_income_band_sk`) DISABLE NOVALIDATE RELY;
ALTER TABLE `customer` ADD CONSTRAINT `c_hd` FOREIGN KEY (`c_current_hdemo_sk`) REFERENCES `household_demographics`(`hd_demo_sk`) DISABLE NOVALIDATE RELY;

EXPLAIN CBO
SELECT i_product_name, i_item_sk
FROM store_sales
    ,customer
    ,household_demographics hd1
    ,household_demographics hd2
    ,income_band ib2
    ,item
WHERE ss_hdemo_sk = hd1.hd_demo_sk AND
      ss_item_sk = i_item_sk and
      c_current_hdemo_sk = hd2.hd_demo_sk AND
      hd2.hd_income_band_sk = ib2.ib_income_band_sk
GROUP BY i_product_name, i_item_sk;
