set hive.mapred.mode=nonstrict;

CREATE TABLE TestV1_Staples (
      Item_Count INT,
      Ship_Priority STRING,
      Order_Priority STRING,
      Order_Status STRING,
      Order_Quantity DOUBLE,
      Sales_Total DOUBLE,
      Discount DOUBLE,
      Tax_Rate DOUBLE,
      Ship_Mode STRING,
      Fill_Time DOUBLE,
      Gross_Profit DOUBLE,
      Price DOUBLE,
      Ship_Handle_Cost DOUBLE,
      Employee_Name STRING,
      Employee_Dept STRING,
      Manager_Name STRING,
      Employee_Yrs_Exp DOUBLE,
      Employee_Salary DOUBLE,
      Customer_Name STRING,
      Customer_State STRING,
      Call_Center_Region STRING,
      Customer_Balance DOUBLE,
      Customer_Segment STRING,
      Prod_Type1 STRING,
      Prod_Type2 STRING,
      Prod_Type3 STRING,
      Prod_Type4 STRING,
      Product_Name STRING,
      Product_Container STRING,
      Ship_Promo STRING,
      Supplier_Name STRING,
      Supplier_Balance DOUBLE,
      Supplier_Region STRING,
      Supplier_State STRING,
      Order_ID STRING,
      Order_Year INT,
      Order_Month INT,
      Order_Day INT,
      Order_Date_ STRING,
      Order_Quarter STRING,
      Product_Base_Margin DOUBLE,
      Product_ID STRING,
      Receive_Time DOUBLE,
      Received_Date_ STRING,
      Ship_Date_ STRING,
      Ship_Charge DOUBLE,
      Total_Cycle_Time DOUBLE,
      Product_In_Stock STRING,
      PID INT,
      Market_Segment STRING
      );

explain
SELECT customer_name, SUM(customer_balance), SUM(order_quantity) FROM default.testv1_staples s1 GROUP BY customer_name HAVING (
(COUNT(s1.discount) <= 822) AND
(SUM(customer_balance) <= 4074689.000000041)
);

explain
SELECT customer_name, SUM(customer_balance), SUM(order_quantity) FROM default.testv1_staples s1 GROUP BY customer_name HAVING (
(SUM(customer_balance) <= 4074689.000000041)
AND (COUNT(s1.discount) <= 822)
);

explain
SELECT s1.customer_name FROM default.testv1_staples s1 join default.src s2 on s1.customer_name = s2.key
GROUP BY s1.customer_name
HAVING (
(SUM(s1.customer_balance) <= 4074689.000000041)
AND (AVG(s1.discount) <= 822)
AND (COUNT(s2.value) > 4)
);

explain
SELECT s1.customer_name FROM default.testv1_staples s1 join default.src s2 on s1.customer_name = s2.key
GROUP BY s1.customer_name, s1.customer_name
HAVING (
(SUM(s1.customer_balance) <= 4074689.000000041)
AND (AVG(s1.discount) <= 822)
AND (COUNT(s2.value) > 4)
);

explain
SELECT distinct s1.customer_name as x, s1.customer_name as y
FROM default.testv1_staples s1 join default.src s2 on s1.customer_name = s2.key
HAVING (
(SUM(s1.customer_balance) <= 4074689.000000041)
AND (AVG(s1.discount) <= 822)
AND (COUNT(s2.value) > 4)
);
