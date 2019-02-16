--! qt:dataset:src

SET hive.ctas.external.tables=false;
create external table nzhang_ctas4 as select key, value from src;


