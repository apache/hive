create table t_n2 (c int);

set hive.default.fileformat.managed=orc;

create table o (c int);

create external table e_n1 (c int) location 'pfile://${system:test.tmp.dir}/foo';

create table i_n0 (c int) location 'pfile://${system:test.tmp.dir}/bar';

set hive.default.fileformat=orc;

create table io (c int);
create external table e2 (c int) location 'pfile://${system:test.tmp.dir}/bar';

describe formatted t_n2;
describe formatted o;
describe formatted io;
describe formatted e_n1;
describe formatted i_n0;
describe formatted e2;

drop table t_n2;
drop table o;
drop table io;
drop table e_n1;
drop table i_n0;
drop table e2;

set hive.default.fileformat=TextFile;
set hive.default.fileformat.managed=none;

create table t_n2 (c int);

set hive.default.fileformat.managed=parquet;

create table o (c int);

create external table e_n1 (c int) location 'pfile://${system:test.tmp.dir}/foo';

create table i_n0 (c int) location 'pfile://${system:test.tmp.dir}/bar';

set hive.default.fileformat=parquet;

create table io (c int);
create external table e2 (c int) location 'pfile://${system:test.tmp.dir}/bar';

describe formatted t_n2;
describe formatted o;
describe formatted io;
describe formatted e_n1;
describe formatted i_n0;
describe formatted e2;

drop table t_n2;
drop table o;
drop table io;
drop table e_n1;
drop table i_n0;
drop table e2;

set hive.default.fileformat=TextFile;
set hive.default.fileformat.managed=none;
