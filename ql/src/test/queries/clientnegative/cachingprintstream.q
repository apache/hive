--! qt:dataset:src
-- Recent JDKs report a failed exec as "Exec failed, error: 2 (No such file or directory) ",
-- older ones as "error=2, No such file or directory"; the expected output uses the older form.
--! qt:replace:/Exec failed, error: (\d+) \(([^)]*)\) ?/error=$1, $2/
set hive.llap.execution.mode=auto;
set hive.exec.failure.hooks=org.apache.hadoop.hive.ql.hooks.VerifyCachingPrintStreamHook;
set hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.VerifyCachingPrintStreamHook;

SELECT count(*) FROM src;
FROM src SELECT TRANSFORM (key, value) USING 'FAKE_SCRIPT_SHOULD_NOT_EXIST' AS key, value;

set hive.exec.failure.hooks=;
set hive.exec.post.hooks=;
