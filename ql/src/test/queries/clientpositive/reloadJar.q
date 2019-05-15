--! qt:dataset:src
dfs -mkdir  ${system:test.tmp.dir}/aux;
dfs -cp ${system:hive.root}/data/files/identity_udf.jar ${system:test.tmp.dir}/aux/udfexample.jar;

SET hive.reloadable.aux.jars.path=${system:test.tmp.dir}/aux;
RELOAD;
CREATE TEMPORARY FUNCTION example_iden AS 'IdentityStringUDF';

EXPLAIN
SELECT example_iden(key)
FROM src LIMIT 1;

SELECT example_iden(key)
FROM src LIMIT 1;

DROP TEMPORARY FUNCTION example_iden;

dfs -rm -r ${system:test.tmp.dir}/aux;
