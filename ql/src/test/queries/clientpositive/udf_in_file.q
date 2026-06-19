DESCRIBE FUNCTION in_file;

CREATE TABLE value_src (str_val char(3), ch_val STRING, vch_val varchar(10),
                        str_val_neg char(3), ch_val_neg STRING, vch_val_neg varchar(10))
       ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '../../data/files/in_file.dat' INTO TABLE value_src;

EXPLAIN
SELECT in_file(str_val, "../../data/files/test2.dat"),
       in_file(ch_val, "../../data/files/test2.dat"),
       in_file(vch_val, "../../data/files/test2.dat"),
       in_file(str_val_neg, "../../data/files/test2.dat"),
       in_file(ch_val_neg, "../../data/files/test2.dat"),
       in_file(vch_val_neg, "../../data/files/test2.dat"),
       in_file("303", "../../data/files/test2.dat"),
       in_file("304", "../../data/files/test2.dat"),
       in_file(CAST(NULL AS STRING), "../../data/files/test2.dat")
FROM value_src LIMIT 1;

SELECT in_file(str_val, "../../data/files/test2.dat"),
       in_file(ch_val, "../../data/files/test2.dat"),
       in_file(vch_val, "../../data/files/test2.dat"),
       in_file(str_val_neg, "../../data/files/test2.dat"),
       in_file(ch_val_neg, "../../data/files/test2.dat"),
       in_file(vch_val_neg, "../../data/files/test2.dat"),
       in_file("303", "../../data/files/test2.dat"),
       in_file("304", "../../data/files/test2.dat"),
       in_file(CAST(NULL AS STRING), "../../data/files/test2.dat")
FROM value_src LIMIT 1;