set hive.cli.errors.ignore=true;

create external table external1(a int, b int) location 'invalidscheme://data.s3ndemo.hive/kv';
describe external1;
