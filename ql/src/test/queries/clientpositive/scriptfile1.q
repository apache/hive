CREATE TABLE scriptfile1_dest1(key INT, value STRING);

ADD FILE src/test/scripts/testgrep;

FROM (
  FROM src
  SELECT TRANSFORM(src.key, src.value)
         USING 'testgrep' AS (tkey, tvalue) 
  CLUSTER BY tkey 
) tmap
INSERT OVERWRITE TABLE scriptfile1_dest1 SELECT tmap.tkey, tmap.tvalue;

SELECT scriptfile1_dest1.* FROM scriptfile1_dest1;
