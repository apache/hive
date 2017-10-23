DROP TABLE IF EXISTS insert_camel_case;
CREATE TABLE insert_camel_case (key int, value string);

INSERT INTO insert_camel_case(KeY, VALuE) SELECT * FROM src LIMIT 100;

DROP TABLE IF EXISTS insert_camel_case;
