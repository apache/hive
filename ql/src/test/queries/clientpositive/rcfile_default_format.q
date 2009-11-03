SET hive.default.fileformat = RCFile;

CREATE TABLE rcfile_default_format (key STRING);
DESCRIBE EXTENDED rcfile_default_format; 

CREATE TABLE rcfile_default_format_ctas AS SELECT key,value FROM src;
DESCRIBE EXTENDED rcfile_default_format_ctas; 

SET hive.default.fileformat = TextFile;
CREATE TABLE textfile_default_format_ctas AS SELECT key,value FROM rcfile_default_format_ctas;
DESCRIBE EXTENDED textfile_default_format_ctas; 