
DESCRIBE FUNCTION deserialize;
DESCRIBE FUNCTION EXTENDED deserialize;

SELECT deserialize("H4sIAAAAAAAA/ytJLS4BAAx+f9gEAAAA", "gzip");
SELECT deserialize("H4sIAAAAAAAA/ytJLS4BAAx+f9gEAAAA", "gzip(json-2.0)");
SELECT deserialize("{unitTest:'udf-deserialize'}", "json-0.2");
