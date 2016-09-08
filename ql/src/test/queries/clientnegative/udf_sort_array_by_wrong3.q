-- invalid field name in side struct

DROP TABLE IF EXISTS sort_array_by_order_wrong;

CREATE TABLE sort_array_by_order_wrong
STORED AS TEXTFILE
AS
SELECT "Google" as company,
        array(
        named_struct('name','Able' ,'salary',28),
        named_struct('name','Boo' ,'salary',70000),
        named_struct('name','Hary' ,'salary',50000)
        ) as employee
;

select company,sort_array_by(employee,'firstName') as col1 from sort_array_by_order_wrong ;
