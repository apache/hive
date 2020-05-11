--! qt:dataset:srcpart
--! qt:dataset:src
-- SORT_QUERY_RESULTS

set hive.mapred.mode=nonstrict;

  SELECT key
    FROM (SELECT '1' AS key FROM srcpart WHERE ds="2008-04-09"
          UNION ALL
          SELECT key FROM srcpart WHERE ds="2008-04-09" AND hr="11") tab
GROUP BY key;

  SELECT key
    FROM (SELECT '1' AS key fROM src
          UNION ALL
          SELECT key AS key FROM src) tab
GROUP BY key;

  SELECT max(key)
    FROM (SELECT '1' AS key FROM src
          UNION ALL
          SELECT key AS key FROM src) tab
GROUP BY key;

  SELECT key
    FROM (SELECT '1' AS key FROM src
          UNION ALL
          SELECT '2' AS key FROM src) tab
GROUP BY key;


  SELECT key
    FROM (SELECT '1' AS key FROM src
          UNION ALL
          SELECT key AS key FROM src
          UNION ALL
          SELECT '2' AS key FROM src
          UNION ALL
          SELECT key AS key FROM src) tab
GROUP BY key;

SELECT k
  FROM (SELECT *
          FROM (SELECT '1' AS k
                  FROM src 
                 LIMIT 0) a
        UNION ALL
        SELECT key AS k
          FROM src
      ORDER BY k
         LIMIT 1) tab;

SELECT k
  FROM (SELECT *
          FROM (SELECT '1' AS k
                  FROM src
                 LIMIT 1) a
        UNION ALL
        SELECT key AS k
          FROM src
         LIMIT 0) tab;

SELECT max(ds) FROM srcpart;

SELECT count(ds) FROM srcpart;


