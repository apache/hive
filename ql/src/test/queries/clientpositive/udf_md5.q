DESCRIBE FUNCTION md5;
DESC FUNCTION EXTENDED md5;

explain select md5('ABC');

select
md5('ABC'),
md5(''),
md5(binary('ABC')),
md5(binary('')),
md5(cast(null as string)),
md5(cast(null as binary)),
md5(null);
