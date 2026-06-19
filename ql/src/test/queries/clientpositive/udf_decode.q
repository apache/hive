DESCRIBE FUNCTION decode;
DESC FUNCTION EXTENDED decode;

explain select decode(binary('TestDecode1'), 'UTF-8');

select
decode(binary('TestDecode1'), 'UTF-8'),
decode(binary('TestDecode2'), cast('UTF-8' as varchar(10))),
decode(binary('TestDecode3'), cast('UTF-8' as char(5))),
decode(cast(null as binary), 'UTF-8');