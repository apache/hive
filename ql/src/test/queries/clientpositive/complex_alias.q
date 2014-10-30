CREATE TABLE agg1 (col0 INT, col1 STRING, col2 DOUBLE);

INSERT INTO TABLE agg1 select key,value,key from src tablesample (1 rows);

EXPLAIN
SELECT single_use_subq11.a1 AS a1,
       single_use_subq11.a2 AS a2
FROM   (SELECT Sum(agg1.col2) AS a1
        FROM   agg1
        GROUP  BY agg1.col0) single_use_subq12
       JOIN (SELECT alias.a2 AS a0,
                    alias.a1 AS a1,
                    alias.a1 AS a2
             FROM   (SELECT agg1.col1 AS a0,
                            '42'      AS a1,
                            agg1.col0 AS a2
                     FROM   agg1
                     UNION ALL
                     SELECT agg1.col1 AS a0,
                            '41'      AS a1,
                            agg1.col0 AS a2
                     FROM   agg1) alias
             GROUP  BY alias.a2,
                       alias.a1) single_use_subq11
         ON ( single_use_subq11.a0 = single_use_subq11.a0 );

SELECT single_use_subq11.a1 AS a1,
       single_use_subq11.a2 AS a2
FROM   (SELECT Sum(agg1.col2) AS a1
        FROM   agg1
        GROUP  BY agg1.col0) single_use_subq12
       JOIN (SELECT alias.a2 AS a0,
                    alias.a1 AS a1,
                    alias.a1 AS a2
             FROM   (SELECT agg1.col1 AS a0,
                            '42'      AS a1,
                            agg1.col0 AS a2
                     FROM   agg1
                     UNION ALL
                     SELECT agg1.col1 AS a0,
                            '41'      AS a1,
                            agg1.col0 AS a2
                     FROM   agg1) alias
             GROUP  BY alias.a2,
                       alias.a1) single_use_subq11
         ON ( single_use_subq11.a0 = single_use_subq11.a0 );
