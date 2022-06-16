create table A (keymap string) partitioned by (ds string);

create table B (keymap string) partitioned by (ds string);

explain extended
select A.ds
from A join B on A.keymap = B.keymap and '2011-10-13' = B.ds;
