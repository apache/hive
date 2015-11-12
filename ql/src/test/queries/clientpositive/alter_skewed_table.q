create table original (key STRING, value STRING);

describe formatted original;

alter table original SKEWED BY (key) ON (1,5,6);

describe formatted original;

drop table original;

create database skew_test;

create table skew_test.original2 (key STRING, value STRING) ;

describe formatted skew_test.original2;

alter table skew_test.original2 SKEWED BY (key, value) ON ((1,1),(5,6));

describe formatted skew_test.original2;

drop table skew_test.original2;

create table skew_test.original3 (key STRING, value STRING) SKEWED BY (key, value) ON ((1,1),(5,6));

describe formatted skew_test.original3;

alter table skew_test.original3 not skewed;

describe formatted skew_test.original3;

drop table skew_test.original3;

drop database skew_test;

