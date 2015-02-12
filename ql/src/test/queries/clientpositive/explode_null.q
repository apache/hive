SELECT explode(col) AS myCol FROM 
    (select * from (SELECT array(1,2,3) AS col FROM src LIMIT 1)ea 
     UNION ALL
     select * from (SELECT IF(false, array(1,2,3), NULL) AS col FROM src LIMIT 1)eb) a;

SELECT explode(col) AS (myCol1,myCol2) FROM
    (select * from (SELECT map(1,'one',2,'two',3,'three') AS col FROM src LIMIT 1)ea
     UNION ALL
     select * from (SELECT IF(false, map(1,'one',2,'two',3,'three'), NULL) AS col FROM src LIMIT 1)eb ) a;
     