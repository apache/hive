
set hive.metastore.disallow.incompatible.col.type.changes=false;

create external table new_char_decimal (c1 char(20));
alter table new_char_decimal change c1 c1 decimal(31,0);

create external table new_varchar_decimal (c1 varchar(25));
alter table new_varchar_decimal change c1 c1 decimal(12,5);

create external table new_char_double (c1 char(20));
alter table new_char_double change c1 c1 double;

create external table new_varchar_double (c1 varchar(25));
alter table new_varchar_double change c1 c1 double;