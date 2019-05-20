--! qt:dataset:src

SELECT src.key, sum(substr(src.value,5)) 
FROM src
GROUP BY src.key
