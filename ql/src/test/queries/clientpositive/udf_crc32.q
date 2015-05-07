DESCRIBE FUNCTION crc32;
DESC FUNCTION EXTENDED crc32;

explain select crc32('ABC');

select
crc32('ABC'),
crc32(''),
crc32(binary('ABC')),
crc32(binary('')),
crc32(cast(null as string)),
crc32(cast(null as binary)),
crc32(null);
