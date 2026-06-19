DESCRIBE FUNCTION aes_encrypt;
DESC FUNCTION EXTENDED aes_encrypt;

explain select aes_encrypt('ABC', '1234567890123456');

select
base64(aes_encrypt('ABC', '1234567890123456')),
base64(aes_encrypt('', '1234567890123456')),
base64(aes_encrypt(binary('ABC'), binary('1234567890123456'))),
base64(aes_encrypt(binary(''), binary('1234567890123456'))),
aes_encrypt(cast(null as string), '1234567890123456'),
aes_encrypt(cast(null as binary), binary('1234567890123456'));

--bad key
select
aes_encrypt('ABC', '12345678901234567'),
aes_encrypt(binary('ABC'), binary('123456789012345')),
aes_encrypt('ABC', ''),
aes_encrypt(binary('ABC'), binary('')),
aes_encrypt('ABC', cast(null as string)),
aes_encrypt(binary('ABC'), cast(null as binary));