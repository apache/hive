DESCRIBE FUNCTION soundex;
DESC FUNCTION EXTENDED soundex;

explain select soundex('Miller');

select
soundex('Miller'),
soundex('miler'),
soundex('myller'),
soundex('muller'),
soundex('m'),
soundex('mu'),
soundex('mul'),
soundex('Peterson'),
soundex('Pittersen'),
soundex(''),
soundex(cast(null as string));