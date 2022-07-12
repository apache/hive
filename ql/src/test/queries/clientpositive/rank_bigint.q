create table int_temp (c1 int);

create table bigint_table as select rank() over() c1_rank  from int_temp;

describe bigint_table;
