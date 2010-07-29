

create table texternal(key string, val string) partitioned by (insertdate string);

!rm -fr /tmp/texternal;
!mkdir -p /tmp/texternal/2008-01-01;

alter table texternal add partition (insertdate='2008-01-01') location 'pfile:///tmp/texternal/2008-01-01';
from src insert overwrite table texternal partition (insertdate='2008-01-01') select *;

select * from texternal where insertdate='2008-01-01';

!rm -fr /tmp/texternal;
