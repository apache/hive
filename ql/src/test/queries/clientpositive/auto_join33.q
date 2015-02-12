set hive.auto.convert.join = true;

-- SORT_QUERY_RESULTS

explain 
SELECT * FROM
  (SELECT * FROM src WHERE key+1 < 10) a
    JOIN 
  (SELECT * FROM src WHERE key+2 < 10) b
    ON a.key+1=b.key+2;

SELECT * FROM
  (SELECT * FROM src WHERE key+1 < 10) a
    JOIN
  (SELECT * FROM src WHERE key+2 < 10) b
    ON a.key+1=b.key+2;
