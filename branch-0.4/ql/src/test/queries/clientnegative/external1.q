set hive.cli.errors.ignore=true;
drop table external1;
create external table external1(a int, b int) location 'invalidscheme://data.s3ndemo.hive/kv';
describe external1;
