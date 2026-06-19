DESCRIBE FUNCTION aes_decrypt;
DESC FUNCTION EXTENDED aes_decrypt;

explain select aes_decrypt(unbase64("y6Ss+zCYObpCbgfWfyNWTw=="), '1234567890123456');

select
aes_decrypt(unbase64("y6Ss+zCYObpCbgfWfyNWTw=="), '1234567890123456'),
aes_decrypt(unbase64("y6Ss+zCYObpCbgfWfyNWTw=="), binary('1234567890123456')),
aes_decrypt(unbase64("BQGHoM3lqYcsurCRq3PlUw=="), '1234567890123456') = binary(''),
aes_decrypt(unbase64("BQGHoM3lqYcsurCRq3PlUw=="), binary('1234567890123456')) = binary(''),
aes_decrypt(cast(null as binary), '1234567890123456'),
aes_decrypt(cast(null as binary), binary('1234567890123456'));

--bad key
select
aes_decrypt(unbase64("y6Ss+zCYObpCbgfWfyNWTw=="), '12345678901234567'),
aes_decrypt(unbase64("y6Ss+zCYObpCbgfWfyNWTw=="), binary('123456789012345')),
aes_decrypt(unbase64("y6Ss+zCYObpCbgfWfyNWTw=="), ''),
aes_decrypt(unbase64("y6Ss+zCYObpCbgfWfyNWTw=="), binary('')),
aes_decrypt(unbase64("y6Ss+zCYObpCbgfWfyNWTw=="), cast(null as string)),
aes_decrypt(unbase64("y6Ss+zCYObpCbgfWfyNWTw=="), cast(null as binary));