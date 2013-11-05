ADD JAR ${system:maven.local.repository}/org/apache/hive/hive-contrib/${system:hive.version}/hive-contrib-${system:hive.version}.jar;

CREATE TEMPORARY FUNCTION dboutput AS 'org.apache.hadoop.hive.contrib.genericudf.example.GenericUDFDBOutput';

set mapred.map.tasks.speculative.execution=false;
set mapred.reduce.tasks.speculative.execution=false;
set mapred.map.tasks=1;
set mapred.reduce.tasks=1;

ADD JAR ${system:maven.local.repository}/org/apache/derby/derby/${system:derby.version}/derby-${system:derby.version}.jar;

DESCRIBE FUNCTION dboutput;

DESCRIBE FUNCTION EXTENDED dboutput;

EXPLAIN FROM src

SELECT dboutput ( 'jdbc:derby:../build/test_dboutput_db\;create=true','','',
'CREATE TABLE app_info ( kkey VARCHAR(255) NOT NULL, vvalue VARCHAR(255) NOT NULL, UNIQUE(kkey))' ),

dboutput('jdbc:derby:../build/test_dboutput_db','','',
'INSERT INTO app_info (kkey,vvalue) VALUES (?,?)','20','a'),

dboutput('jdbc:derby:../build/test_dboutput_db','','',
'INSERT INTO app_info (kkey,vvalue) VALUES (?,?)','20','b')

limit 1;


FROM src 

SELECT dboutput ( 'jdbc:derby:../build/test_dboutput_db\;create=true','','',
'CREATE TABLE app_info ( kkey INTEGER NOT NULL, vvalue VARCHAR(255) NOT NULL, UNIQUE(kkey))' ),

dboutput('jdbc:derby:../build/test_dboutput_db','','',
'INSERT INTO app_info (kkey,vvalue) VALUES (?,?)','20','a'),

dboutput('jdbc:derby:../build/test_dboutput_db','','',
'INSERT INTO app_info (kkey,vvalue) VALUES (?,?)','20','b')

limit 1;

EXPLAIN SELECT

dboutput('jdbc:derby:../build/test_dboutput_db','','',
'INSERT INTO app_info (kkey,vvalue) VALUES (?,?)',key,value)

FROM src WHERE key < 10;


SELECT

dboutput('jdbc:derby:../build/test_dboutput_db','','',
'INSERT INTO app_info (kkey,vvalue) VALUES (?,?)',key,value)

FROM src WHERE key < 10;

dfs -rmr ../build/test_dboutput_db;
dfs -rmr derby.log;

DROP TEMPORARY FUNCTION dboutput;
