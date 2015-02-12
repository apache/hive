-- These test attempts to deserialize an Avro file that contains map null values, and the file schema
-- vs record schema have the null values in different positions
-- i.e.
-- fileSchema   = [{ "type" : "map", "values" : ["string","null"]}, "null"]
-- recordSchema = ["null", { "type" : "map", "values" : ["string","null"]}]

-- JAVA_VERSION_SPECIFIC_OUTPUT

DROP TABLE IF EXISTS avro_table;

CREATE TABLE avro_table (avreau_col_1 map<string,string>) STORED AS AVRO;
LOAD DATA LOCAL INPATH '../../data/files/map_null_val.avro' OVERWRITE INTO TABLE avro_table;
SELECT * FROM avro_table;

DROP TABLE avro_table;
