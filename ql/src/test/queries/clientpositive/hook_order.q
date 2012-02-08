SET hive.exec.pre.hooks=org.apache.hadoop.hive.ql.hooks.VerifyHooksRunInOrder$RunFirst,org.apache.hadoop.hive.ql.hooks.VerifyHooksRunInOrder$RunSecond;
SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.VerifyHooksRunInOrder$RunFirst,org.apache.hadoop.hive.ql.hooks.VerifyHooksRunInOrder$RunSecond;
SET hive.semantic.analyzer.hook=org.apache.hadoop.hive.ql.hooks.VerifyHooksRunInOrder$RunFirstSemanticAnalysisHook,org.apache.hadoop.hive.ql.hooks.VerifyHooksRunInOrder$RunSecondSemanticAnalysisHook;

SELECT count(*) FROM src;

SET hive.exec.pre.hooks=;
SET hive.exec.post.hooks=;
SET hive.semantic.analyzer.hook=;
