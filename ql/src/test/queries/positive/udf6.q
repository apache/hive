--! qt:dataset:src
FROM src SELECT CONCAT('a', 'b'), IF(TRUE, 1 ,2) + key
