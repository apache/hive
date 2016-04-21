set hive.cbo.enable=false;
set hive.ppd.remove.duplicatefilters=true;
set hive.optimize.ppd=true;

DROP TABLE IF EXISTS ttest1;
DROP TABLE IF EXISTS ttest2;
CREATE TABLE ttest1 (
  `id1` bigint COMMENT 'from deserializer',
  `ts1` string COMMENT 'from deserializer',
  `dt1` string COMMENT 'from deserializer',
  `dt2` string COMMENT 'from deserializer',
  `ac1` string COMMENT 'from deserializer',
  `kd1` string COMMENT 'from deserializer',
  `sts` string COMMENT 'from deserializer',
  `at1` bigint COMMENT 'from deserializer');

CREATE TABLE ttest2 (
  `id1` bigint,
  `ts1` string,
  `dt1` string,
  `dt2` string,
  `ac1` string,
  `kd1` string,
  `sts` string,
  `at1` bigint,
  `khash` string,
  `rhash` string);

explain SELECT
  A2.id1, A2.sts,A2.at1,
    CASE WHEN FLAG = 'A_INS' THEN date_add('2015-11-20', 1) ELSE '2015-11-20' END dt1
        ,A2.dt2
        ,A2.khash
        ,A2.rhash
        ,A2.FLAG
  FROM (
   SELECT
  A2.id1, A2.sts,A2.at1
        ,A2.dt1
        ,A2.dt2
        ,A2.khash
        ,A2.rhash
    ,CASE
     WHEN A2.khash IS NOT NULL
     AND A1.khash IS NULL
         AND A2.dt1 >= '2016-02-05'
     THEN 'DEL'

         WHEN A2.khash IS NOT NULL
     AND A1.khash IS NULL
     AND A2.dt1 <= '2016-02-05'
     THEN 'RET'

     WHEN
     (
     A2.khash = A1.khash
     AND A2.rhash <> A1.rhash
     )
     THEN 'A_INS'

     ELSE 'NA'
    END FLAG
        FROM (
                SELECT *
                        ,reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',concat(id1)) khash
                        ,reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',concat(at1)) rhash
                FROM ttest1
                WHERE ts1 = '2015-11-20'
                ) A1
        FULL OUTER JOIN (
                SELECT *
                FROM ttest2
                WHERE '2015-11-20' BETWEEN dt1 AND dt2
                ) A2
     ON A1.khash = A2.khash
     WHERE NOT (
        NVL(A1.khash, - 1) = NVL(A2.khash, - 1)
        AND NVL(A1.rhash, - 1) = NVL(A2.rhash, - 1)
        )
     AND A2.khash IS NOT NULL

   UNION ALL

   SELECT A1.id1, A1.sts,A1.at1
        ,A1.dt1
                , '2099-12-31' dt2
        ,A1.khash
        ,A1.rhash
     ,CASE WHEN A2.khash IS NOT NULL
     AND A1.khash IS NULL
     AND A2.ts1 <= A1.ts1
     THEN 'DEL'

     WHEN ( A2.khash IS NULL AND A1.khash IS NOT NULL )
     OR ( A2.khash = A1.khash AND A2.rhash <> A1.rhash ) THEN 'INS' ELSE 'NA' END FLAG
   FROM (
     SELECT *
        ,reflect('org.apache.commons.codec.digest.DigestUtils', 'sha256Hex',concat(id1)) khash
        ,reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',concat(at1)) rhash
      FROM ttest1
     WHERE ts1 = '2015-11-20'
    ) A1
   FULL OUTER JOIN (
        SELECT *
        FROM ttest2
        WHERE '2015-11-20' BETWEEN dt1
          AND dt2
       ) A2 ON A1.khash = A2.khash
       WHERE NOT (
          NVL(A1.khash, - 1) = NVL(A2.khash, - 1)
          AND NVL(A1.rhash, - 1) = NVL(A2.rhash, - 1)
          )
       AND A1.khash IS NOT NULL
   ) A2
   where a2.flag <> 'RET';

set hive.cbo.enable=true;
set hive.ppd.remove.duplicatefilters=true;
set hive.optimize.ppd=true;

explain SELECT
  A2.id1, A2.sts,A2.at1,
    CASE WHEN FLAG = 'A_INS' THEN date_add('2015-11-20', 1) ELSE '2015-11-20' END dt1
        ,A2.dt2
        ,A2.khash
        ,A2.rhash
        ,A2.FLAG
  FROM (
   SELECT
  A2.id1, A2.sts,A2.at1
        ,A2.dt1
        ,A2.dt2
        ,A2.khash
        ,A2.rhash
    ,CASE
     WHEN A2.khash IS NOT NULL
     AND A1.khash IS NULL
         AND A2.dt1 >= '2016-02-05'
     THEN 'DEL'

         WHEN A2.khash IS NOT NULL
     AND A1.khash IS NULL
     AND A2.dt1 <= '2016-02-05'
     THEN 'RET'

     WHEN
     (
     A2.khash = A1.khash
     AND A2.rhash <> A1.rhash
     )
     THEN 'A_INS'

     ELSE 'NA'
    END FLAG
        FROM (
                SELECT *
                        ,reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',concat(id1)) khash
                        ,reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',concat(at1)) rhash
                FROM ttest1
                WHERE ts1 = '2015-11-20'
                ) A1
        FULL OUTER JOIN (
                SELECT *
                FROM ttest2
                WHERE '2015-11-20' BETWEEN dt1 AND dt2
                ) A2
     ON A1.khash = A2.khash
     WHERE NOT (
        NVL(A1.khash, - 1) = NVL(A2.khash, - 1)
        AND NVL(A1.rhash, - 1) = NVL(A2.rhash, - 1)
        )
     AND A2.khash IS NOT NULL

   UNION ALL

   SELECT A1.id1, A1.sts,A1.at1
        ,A1.dt1
                , '2099-12-31' dt2
        ,A1.khash
        ,A1.rhash
     ,CASE WHEN A2.khash IS NOT NULL
     AND A1.khash IS NULL
     AND A2.ts1 <= A1.ts1
     THEN 'DEL'

     WHEN ( A2.khash IS NULL AND A1.khash IS NOT NULL )
     OR ( A2.khash = A1.khash AND A2.rhash <> A1.rhash ) THEN 'INS' ELSE 'NA' END FLAG
   FROM (
     SELECT *
        ,reflect('org.apache.commons.codec.digest.DigestUtils', 'sha256Hex',concat(id1)) khash
        ,reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',concat(at1)) rhash
      FROM ttest1
     WHERE ts1 = '2015-11-20'
    ) A1
   FULL OUTER JOIN (
        SELECT *
        FROM ttest2
        WHERE '2015-11-20' BETWEEN dt1
          AND dt2
       ) A2 ON A1.khash = A2.khash
       WHERE NOT (
          NVL(A1.khash, - 1) = NVL(A2.khash, - 1)
          AND NVL(A1.rhash, - 1) = NVL(A2.rhash, - 1)
          )
       AND A1.khash IS NOT NULL
   ) A2
   where a2.flag <> 'RET';

set hive.cbo.enable=false;
set hive.ppd.remove.duplicatefilters=false;
set hive.optimize.ppd=true;

explain SELECT
  A2.id1, A2.sts,A2.at1,
    CASE WHEN FLAG = 'A_INS' THEN date_add('2015-11-20', 1) ELSE '2015-11-20' END dt1
        ,A2.dt2
        ,A2.khash
        ,A2.rhash
        ,A2.FLAG
  FROM (
   SELECT
  A2.id1, A2.sts,A2.at1
        ,A2.dt1
        ,A2.dt2
        ,A2.khash
        ,A2.rhash
    ,CASE
     WHEN A2.khash IS NOT NULL
     AND A1.khash IS NULL
         AND A2.dt1 >= '2016-02-05'
     THEN 'DEL'

         WHEN A2.khash IS NOT NULL
     AND A1.khash IS NULL
     AND A2.dt1 <= '2016-02-05'
     THEN 'RET'

     WHEN
     (
     A2.khash = A1.khash
     AND A2.rhash <> A1.rhash
     )
     THEN 'A_INS'

     ELSE 'NA'
    END FLAG
        FROM (
                SELECT *
                        ,reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',concat(id1)) khash
                        ,reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',concat(at1)) rhash
                FROM ttest1
                WHERE ts1 = '2015-11-20'
                ) A1
        FULL OUTER JOIN (
                SELECT *
                FROM ttest2
                WHERE '2015-11-20' BETWEEN dt1 AND dt2
                ) A2
     ON A1.khash = A2.khash
     WHERE NOT (
        NVL(A1.khash, - 1) = NVL(A2.khash, - 1)
        AND NVL(A1.rhash, - 1) = NVL(A2.rhash, - 1)
        )
     AND A2.khash IS NOT NULL

   UNION ALL

   SELECT A1.id1, A1.sts,A1.at1
        ,A1.dt1
                , '2099-12-31' dt2
        ,A1.khash
        ,A1.rhash
     ,CASE WHEN A2.khash IS NOT NULL
     AND A1.khash IS NULL
     AND A2.ts1 <= A1.ts1
     THEN 'DEL'

     WHEN ( A2.khash IS NULL AND A1.khash IS NOT NULL )
     OR ( A2.khash = A1.khash AND A2.rhash <> A1.rhash ) THEN 'INS' ELSE 'NA' END FLAG
   FROM (
     SELECT *
        ,reflect('org.apache.commons.codec.digest.DigestUtils', 'sha256Hex',concat(id1)) khash
        ,reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',concat(at1)) rhash
      FROM ttest1
     WHERE ts1 = '2015-11-20'
    ) A1
   FULL OUTER JOIN (
        SELECT *
        FROM ttest2
        WHERE '2015-11-20' BETWEEN dt1
          AND dt2
       ) A2 ON A1.khash = A2.khash
       WHERE NOT (
          NVL(A1.khash, - 1) = NVL(A2.khash, - 1)
          AND NVL(A1.rhash, - 1) = NVL(A2.rhash, - 1)
          )
       AND A1.khash IS NOT NULL
   ) A2
   where a2.flag <> 'RET';

set hive.cbo.enable=false;
set hive.optimize.ppd=false;
explain SELECT
  A2.id1, A2.sts,A2.at1,
    CASE WHEN FLAG = 'A_INS' THEN date_add('2015-11-20', 1) ELSE '2015-11-20' END dt1
        ,A2.dt2
        ,A2.khash
        ,A2.rhash
        ,A2.FLAG
  FROM (
   SELECT
  A2.id1, A2.sts,A2.at1
        ,A2.dt1
        ,A2.dt2
        ,A2.khash
        ,A2.rhash
    ,CASE
     WHEN A2.khash IS NOT NULL
     AND A1.khash IS NULL
         AND A2.dt1 >= '2016-02-05'
     THEN 'DEL'

         WHEN A2.khash IS NOT NULL
     AND A1.khash IS NULL
     AND A2.dt1 <= '2016-02-05'
     THEN 'RET'

     WHEN
     (
     A2.khash = A1.khash
     AND A2.rhash <> A1.rhash
     )
     THEN 'A_INS'

     ELSE 'NA'
    END FLAG
        FROM (
                SELECT *
                        ,reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',concat(id1)) khash
                        ,reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',concat(at1)) rhash
                FROM ttest1
                WHERE ts1 = '2015-11-20'
                ) A1
        FULL OUTER JOIN (
                SELECT *
                FROM ttest2
                WHERE '2015-11-20' BETWEEN dt1 AND dt2
                ) A2
     ON A1.khash = A2.khash
     WHERE NOT (
        NVL(A1.khash, - 1) = NVL(A2.khash, - 1)
        AND NVL(A1.rhash, - 1) = NVL(A2.rhash, - 1)
        )
     AND A2.khash IS NOT NULL

   UNION ALL

   SELECT A1.id1, A1.sts,A1.at1
        ,A1.dt1
                , '2099-12-31' dt2
        ,A1.khash
        ,A1.rhash
     ,CASE WHEN A2.khash IS NOT NULL
     AND A1.khash IS NULL
     AND A2.ts1 <= A1.ts1
     THEN 'DEL'

     WHEN ( A2.khash IS NULL AND A1.khash IS NOT NULL )
     OR ( A2.khash = A1.khash AND A2.rhash <> A1.rhash ) THEN 'INS' ELSE 'NA' END FLAG
   FROM (
     SELECT *
        ,reflect('org.apache.commons.codec.digest.DigestUtils', 'sha256Hex',concat(id1)) khash
        ,reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',concat(at1)) rhash
      FROM ttest1
     WHERE ts1 = '2015-11-20'
    ) A1
   FULL OUTER JOIN (
        SELECT *
        FROM ttest2
        WHERE '2015-11-20' BETWEEN dt1
          AND dt2
       ) A2 ON A1.khash = A2.khash
       WHERE NOT (
          NVL(A1.khash, - 1) = NVL(A2.khash, - 1)
          AND NVL(A1.rhash, - 1) = NVL(A2.rhash, - 1)
          )
       AND A1.khash IS NOT NULL
   ) A2
   where a2.flag <> 'RET';

set hive.cbo.enable=true;
set hive.optimize.ppd=false;
explain SELECT
  A2.id1, A2.sts,A2.at1,
    CASE WHEN FLAG = 'A_INS' THEN date_add('2015-11-20', 1) ELSE '2015-11-20' END dt1
        ,A2.dt2
        ,A2.khash
        ,A2.rhash
        ,A2.FLAG
  FROM (
   SELECT
  A2.id1, A2.sts,A2.at1
        ,A2.dt1
        ,A2.dt2
        ,A2.khash
        ,A2.rhash
    ,CASE
     WHEN A2.khash IS NOT NULL
     AND A1.khash IS NULL
         AND A2.dt1 >= '2016-02-05'
     THEN 'DEL'

         WHEN A2.khash IS NOT NULL
     AND A1.khash IS NULL
     AND A2.dt1 <= '2016-02-05'
     THEN 'RET'

     WHEN
     (
     A2.khash = A1.khash
     AND A2.rhash <> A1.rhash
     )
     THEN 'A_INS'

     ELSE 'NA'
    END FLAG
        FROM (
                SELECT *
                        ,reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',concat(id1)) khash
                        ,reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',concat(at1)) rhash
                FROM ttest1
                WHERE ts1 = '2015-11-20'
                ) A1
        FULL OUTER JOIN (
                SELECT *
                FROM ttest2
                WHERE '2015-11-20' BETWEEN dt1 AND dt2
                ) A2
     ON A1.khash = A2.khash
     WHERE NOT (
        NVL(A1.khash, - 1) = NVL(A2.khash, - 1)
        AND NVL(A1.rhash, - 1) = NVL(A2.rhash, - 1)
        )
     AND A2.khash IS NOT NULL

   UNION ALL

   SELECT A1.id1, A1.sts,A1.at1
        ,A1.dt1
                , '2099-12-31' dt2
        ,A1.khash
        ,A1.rhash
     ,CASE WHEN A2.khash IS NOT NULL
     AND A1.khash IS NULL
     AND A2.ts1 <= A1.ts1
     THEN 'DEL'

     WHEN ( A2.khash IS NULL AND A1.khash IS NOT NULL )
     OR ( A2.khash = A1.khash AND A2.rhash <> A1.rhash ) THEN 'INS' ELSE 'NA' END FLAG
   FROM (
     SELECT *
        ,reflect('org.apache.commons.codec.digest.DigestUtils', 'sha256Hex',concat(id1)) khash
        ,reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',concat(at1)) rhash
      FROM ttest1
     WHERE ts1 = '2015-11-20'
    ) A1
   FULL OUTER JOIN (
        SELECT *
        FROM ttest2
        WHERE '2015-11-20' BETWEEN dt1
          AND dt2
       ) A2 ON A1.khash = A2.khash
       WHERE NOT (
          NVL(A1.khash, - 1) = NVL(A2.khash, - 1)
          AND NVL(A1.rhash, - 1) = NVL(A2.rhash, - 1)
          )
       AND A1.khash IS NOT NULL
   ) A2
   where a2.flag <> 'RET';

DROP TABLE ttest1;
DROP TABLE ttest2;

