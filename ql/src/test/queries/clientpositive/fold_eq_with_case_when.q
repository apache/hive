--! qt:dataset:src
--! qt:dataset:lineitem
set hive.mapred.mode=nonstrict;
explain
SELECT  
SUM((CASE WHEN 1000000 = 0 THEN NULL ELSE l_partkey / 1000000 END)),
SUM(1) AS `sum_number_of_records_ok` FROM lineitem  
WHERE 
(((CASE WHEN ('N' = l_returnflag) THEN 1 ELSE 1 END) = 1) AND 
((CASE WHEN ('MAIL' = l_shipmode) THEN 1 ELSE 1 END) = 1) AND 
((CASE WHEN ('O' = l_linestatus) THEN 1 ELSE 1 END) = 1) AND 
((CASE WHEN ('NONE' = l_shipinstruct) THEN 1 ELSE 1 END) = 1) AND  
((CASE WHEN ('All' = (CASE WHEN (l_shipmode = 'TRUCK') THEN 'East' WHEN (l_shipmode = 'MAIL') THEN 'West' WHEN (l_shipmode = 'REG AIR') THEN 'BizDev' ELSE 'Other' END)) THEN 1 ELSE 1 END) = 1) AND 
((CASE WHEN ('AIR' = l_shipmode) THEN 1 ELSE 1 END) = 1) AND 
((CASE WHEN ('1996-03-30' = TO_DATE(l_shipdate)) THEN 1 ELSE NULL END) = 1) AND  
((CASE WHEN ('RAIL' = l_shipmode) THEN 1 ELSE NULL END) = 1) AND (1 = 1) AND 
((CASE WHEN (1 = l_linenumber) THEN 1 ELSE 1 END) = 1) AND (1 = 1)) 
GROUP BY l_orderkey;


explain select key from src where (case key when '238' then 1 else 2 end) = 1; 
explain select key from src where (case key when '238' then 1  when '94' then 1 else 3 end) = cast('1' as int); 
explain select key from src where (case key when '238' then 1 else 2 end) = (case when key != '238' then 1 else 1 end); 
explain select key from src where (case key when '238' then 1 end) = (case when key != '238' then 1 when key = '23' then 1 end); 
