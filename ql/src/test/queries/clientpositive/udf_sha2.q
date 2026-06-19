DESCRIBE FUNCTION sha2;
DESC FUNCTION EXTENDED sha2;

explain select sha2('ABC', 256);

select
sha2('ABC', 0),
sha2('', 0),
sha2(binary('ABC'), 0),
sha2(binary(''), 0),
sha2(cast(null as string), 0),
sha2(cast(null as binary), 0);

select
sha2('ABC', 256),
sha2('', 256),
sha2(binary('ABC'), 256),
sha2(binary(''), 256),
sha2(cast(null as string), 256),
sha2(cast(null as binary), 256);

select
sha2('ABC', 384),
sha2('', 384),
sha2(binary('ABC'), 384),
sha2(binary(''), 384),
sha2(cast(null as string), 384),
sha2(cast(null as binary), 384);

select
sha2('ABC', 512),
sha2('', 512),
sha2(binary('ABC'), 512),
sha2(binary(''), 512),
sha2(cast(null as string), 512),
sha2(cast(null as binary), 512);

--null
select
sha2('ABC', 200),
sha2('ABC', cast(null as int));