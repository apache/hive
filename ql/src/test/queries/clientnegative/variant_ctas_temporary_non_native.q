-- of the temporary tables, only native ones may hold a variant: a storage handler writes to its own system
CREATE TEMPORARY TABLE variant_ctas_handler STORED BY 'org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler'
AS SELECT parse_json('{"a":1}') AS v;
