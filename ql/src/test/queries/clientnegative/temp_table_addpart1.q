
create temporary table addpart1_temp (a int) partitioned by (b string, c string);

alter table addpart1_temp add partition (b='f', c='s');

show partitions addpart1_temp;

alter table addpart1_temp add partition (b='f', c='');

show prtitions addpart1_temp;

