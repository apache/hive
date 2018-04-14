--! qt:dataset:src
create table src_multi1 (a STRING NOT NULL ENFORCED, b STRING);
create table src_multi2 (i STRING, j STRING NOT NULL ENFORCED);

from src
insert overwrite table src_multi1 select * where key < 10
insert overwrite table src_multi2 select key, null where key > 10 and key < 20;
