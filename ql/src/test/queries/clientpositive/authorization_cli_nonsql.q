--! qt:dataset:src
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory;
set hive.security.authorization.enabled=false;

-- Verify that dfs,compile,add,delete commands can be run from hive cli, and no authorization checks happen when auth is diabled

use default;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/a_admin_almighty1;
dfs -ls ${system:test.tmp.dir}/a_admin_almighty1;

create table a_table1_n0(a int, b int);
add jar ${system:maven.local.repository}/org/apache/hive/hive-it-test-serde/${system:hive.version}/hive-it-test-serde-${system:hive.version}.jar;
alter table a_table1_n0 set serde 'org.apache.hadoop.hive.serde2.TestSerDe' with serdeproperties('s1'='9');
drop table a_table;

delete jar ${system:maven.local.repository}/org/apache/hive/hive-it-test-serde/${system:hive.version}/hive-it-test-serde-${system:hive.version}.jar;

compile `import org.apache.hadoop.hive.ql.exec.UDF \;
public class Pyth extends UDF {
  public double evaluate(double a, double b){
    return Math.sqrt((a*a) + (b*b)) \;
  }
} `AS GROOVY NAMED Pyth.groovy;
CREATE TEMPORARY FUNCTION Pyth as 'Pyth';

SELECT Pyth(3,4) FROM src tablesample (1 rows);

DROP TEMPORARY FUNCTION Pyth;

