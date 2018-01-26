UPDATE TABLE_PARAMS
  SET PARAM_KEY = 'hbase.mapreduce.hfileoutputformat.table.name'
WHERE
      PARAM_KEY = 'hbase.table.name'
;

