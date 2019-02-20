-- aggregates are not allowed
create table tti(i int check (sum(i) > 5));
