SET hive.default.fileformat = RCFile;

CREATE TABLE rcfile_default_format (key STRING);
DESCRIBE EXTENDED rcfile_default_format; 

CREATE TABLE rcfile_default_format_ctas AS SELECT key,value FROM src;
DESCRIBE EXTENDED rcfile_default_format_ctas; 

CREATE TABLE rcfile_default_format_txtfile (key STRING) STORED AS TEXTFILE;
INSERT OVERWRITE TABLE rcfile_default_format_txtfile SELECT key from src;
DESCRIBE EXTENDED rcfile_default_format_txtfile; 

SET hive.default.fileformat = TextFile;
CREATE TABLE textfile_default_format_ctas AS SELECT key,value FROM rcfile_default_format_ctas;
DESCRIBE EXTENDED textfile_default_format_ctas;

DROP TABLE  rcfile_default_format;
DROP TABLE  rcfile_default_format_ctas;
DROP TABLE rcfile_default_format_txtfile;
DROP TABLE textfile_default_format_ctas;