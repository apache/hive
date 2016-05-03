DESCRIBE FUNCTION mask_hash;
DESC FUNCTION EXTENDED mask_hash;

explain select mask_hash('TestString-123');

select mask_hash('TestString-123'),
       mask_hash(cast('TestString-123' as varchar(24))),
       mask_hash(cast('TestString-123' as char(24))),
       mask_hash(cast(123 as tinyint)),
       mask_hash(cast(12345 as smallint)),
       mask_hash(cast(12345 as int)),
       mask_hash(cast(12345 as bigint)),
       mask_hash(cast('2016-04-20' as date));
