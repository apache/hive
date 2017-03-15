create table t (c int);

set hive.default.fileformat.managed=orc;

create table o (c int);

create external table e (c int) location 'pfile://${system:test.tmp.dir}/foo';

create table i (c int) location 'pfile://${system:test.tmp.dir}/bar';

set hive.default.fileformat=orc;

create table io (c int);
create external table e2 (c int) location 'pfile://${system:test.tmp.dir}/bar';

describe formatted t;
describe formatted o;
describe formatted io;
describe formatted e;
describe formatted i;
describe formatted e2;

drop table t;
drop table o;
drop table io;
drop table e;
drop table i;
drop table e2;

set hive.default.fileformat=TextFile;
set hive.default.fileformat.managed=none;

create table t (c int);

set hive.default.fileformat.managed=parquet;

create table o (c int);

create external table e (c int) location 'pfile://${system:test.tmp.dir}/foo';

create table i (c int) location 'pfile://${system:test.tmp.dir}/bar';

set hive.default.fileformat=parquet;

create table io (c int);
create external table e2 (c int) location 'pfile://${system:test.tmp.dir}/bar';

describe formatted t;
describe formatted o;
describe formatted io;
describe formatted e;
describe formatted i;
describe formatted e2;

drop table t;
drop table o;
drop table io;
drop table e;
drop table i;
drop table e2;

set hive.default.fileformat=TextFile;
set hive.default.fileformat.managed=none;
