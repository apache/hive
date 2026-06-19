DESCRIBE FUNCTION sha1;
DESC FUNCTION EXTENDED sha;

explain select sha1('ABC');

select
sha1('ABC'),
sha(''),
sha(binary('ABC')),
sha1(binary('')),
sha1(cast(null as string)),
sha(cast(null as binary)),
sha1(null);
