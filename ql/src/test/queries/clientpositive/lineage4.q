create table table_t (id int) partitioned by (dob date);
create table table_b (id int) partitioned by (dob date);
from table_b a insert overwrite table table_t select a.id,a.dob;
