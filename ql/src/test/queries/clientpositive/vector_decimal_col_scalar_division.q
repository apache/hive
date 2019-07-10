CREATE temporary TABLE `catalog_Sales`(
  `cs_quantity` int,
  `cs_wholesale_cost` decimal(7,2),
  `cs_list_price` decimal(7,2),
  `cs_sales_price` decimal(7,2),
  `cs_ext_discount_amt` decimal(7,2),
  `cs_ext_sales_price` decimal(7,2),
  `cs_ext_wholesale_cost` decimal(7,2),
  `cs_ext_list_price` decimal(7,2),
  `cs_ext_tax` decimal(7,2),
  `cs_coupon_amt` decimal(7,2),
  `cs_ext_ship_cost` decimal(7,2),
  `cs_net_paid` decimal(7,2),
  `cs_net_paid_inc_tax` decimal(7,2),
  `cs_net_paid_inc_ship` decimal(7,2),
  `cs_net_paid_inc_ship_tax` decimal(7,2),
  `cs_net_profit` decimal(7,2))
 ;

explain vectorization detail select max((((cs_ext_list_price - cs_ext_wholesale_cost) - cs_ext_discount_amt) + cs_ext_sales_price) / 2) from catalog_sales;
