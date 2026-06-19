--! qt:dataset:src
select get_splits("SELECT * FROM src WHERE value in (SELECT value FROM src)",0);
select get_splits("SELECT key AS `key 1`, value AS `value 1` FROM src",0);
