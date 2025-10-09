--! qt:dataset:src

SET hive.insert.into.multilevel.dirs=true;
SET hive.output.file.extension=.txt;

set hive.ranger.use.fully.qualified.url = true;
INSERT OVERWRITE DIRECTORY 'target/data/x/y/z/' SELECT src.* FROM src;

set hive.ranger.use.fully.qualified.url = false;
INSERT OVERWRITE DIRECTORY 'target/data/x/y/z/' SELECT src.* FROM src;
