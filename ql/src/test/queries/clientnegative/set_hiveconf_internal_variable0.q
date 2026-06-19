--! qt:dataset:src
-- should fail: for some internal variables which should not be settable via set command
desc src;

set hive.added.jars.path=file://rootdir/test/added/a.jar;
