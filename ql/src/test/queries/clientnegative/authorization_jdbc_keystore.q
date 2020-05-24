--! qt:authorizer
--! qt:dataset:


dfs -cp ${system:test.tmp.dir}/../../../../data/files/test.jceks ${system:test.tmp.dir}/test.jceks;
dfs -chmod 555 ${system:test.tmp.dir}/test.jceks;

CREATE EXTERNAL TABLE ext_auth1
(
 ikey int,
 bkey bigint,
 fkey float,
 dkey double
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_auth1;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user1",
                "hive.sql.dbcp.password.keystore" = "jceks://file/${system:test.tmp.dir}/test.jceks",
                "hive.sql.dbcp.password.key" = "test_derby_auth1.password",
                "hive.sql.table" = "SIMPLE_DERBY_TABLE1",
                "hive.sql.dbcp.maxActive" = "1"
);
