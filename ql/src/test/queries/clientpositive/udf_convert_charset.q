DESCRIBE FUNCTION convertCharset;
DESC FUNCTION EXTENDED convertCharset;

explain select convertCharset('TestConvertCharset1', 'UTF-8', 'US-ASCII');

select
convertCharset('TestConvertCharset1', 'UTF-8', 'US-ASCII'),
convertCharset('TestConvertCharset2', cast('UTF-8' as varchar(10)), 'US-ASCII'),
convertCharset('TestConvertCharset3', cast('UTF-8' as char(5)), 'US-ASCII');
