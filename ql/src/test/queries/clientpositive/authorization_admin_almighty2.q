--! qt:dataset:src
set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set user.name=hive_admin_user;

-- test commands such as dfs,add,delete,compile allowed only by admin user, after following statement
use default;

set role admin;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/a_admin_almighty1;
dfs -ls ${system:test.tmp.dir}/a_admin_almighty1;

create table a_table1(a int, b int);
add jar ${system:maven.local.repository}/org/apache/hive/hive-it-test-serde/${system:hive.version}/hive-it-test-serde-${system:hive.version}.jar;
alter table a_table1 set serde 'org.apache.hadoop.hive.serde2.TestSerDe' with serdeproperties('s1'='9');
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

