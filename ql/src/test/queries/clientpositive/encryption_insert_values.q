-- SORT_QUERY_RESULTS;

DROP TABLE IF EXISTS encrypted_table PURGE;
CREATE TABLE encrypted_table (key INT, value STRING) LOCATION '${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_table';
CRYPTO CREATE_KEY --keyName key_128 --bitLength 128;
CRYPTO CREATE_ZONE --keyName key_128 --path ${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_table;

INSERT INTO encrypted_table values(1,'foo'),(2,'bar');

select * from encrypted_table;

-- this checks that we've actually created temp table data under encrypted_table folder 
describe formatted values__tmp__table__1;

CRYPTO DELETE_KEY --keyName key_128;