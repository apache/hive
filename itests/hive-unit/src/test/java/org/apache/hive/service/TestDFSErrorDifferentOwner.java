package org.apache.hive.service;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import static org.junit.Assert.assertTrue;

public class TestDFSErrorDifferentOwner {


    private static MiniHS2 miniHS2 = null;
    private static HiveConf hiveConf = null;

    @BeforeClass
    public static void startServices() throws Exception {
        hiveConf = new HiveConf();
        hiveConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_MIN_WORKER_THREADS, 1);
        hiveConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_MAX_WORKER_THREADS, 1);
        hiveConf.setBoolVar(HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI, true);
        hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
        hiveConf.set("hive.load.data.owner","hive");
        // Setting hive.server2.enable.doAs to True ensures that HS2 performs the query operation as
        // the connected user instead of the user running HS2.
        hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS, true);

        miniHS2 = new MiniHS2.Builder()
                .withMiniMR()
                .withRemoteMetastore()
                .withConf(hiveConf).build();

        miniHS2.start(new HashMap<String, String>());
    }

    @AfterClass
    public static void stopServices() throws Exception {
        if (miniHS2 != null && miniHS2.isStarted()) {
            miniHS2.stop();
        }
    }

    @Test
    public void testAccessDenied() throws Exception {
        assertTrue("Test setup failed. MiniHS2 is not initialized",
                miniHS2 != null && miniHS2.isStarted());

        Class.forName(MiniHS2.getJdbcDriverName());
        Path scratchDir = new Path(HiveConf.getVar(hiveConf, HiveConf.ConfVars.SCRATCHDIR));

        HadoopShims.MiniDFSShim dfs = miniHS2.getDfs();
        FileSystem fs = dfs.getFileSystem();

        Path stickyBitDir = new Path(scratchDir, "stickyBitDir");

        fs.mkdirs(stickyBitDir);

        String dataFileDir = hiveConf.get("test.data.files").replace('\\', '/')
                .replace("c:", "").replace("C:", "").replace("D:", "").replace("d:", "");
        Path dataFilePath = new Path(dataFileDir, "kv1.txt");

        fs.copyFromLocalFile(dataFilePath, stickyBitDir);

        FsPermission fsPermission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.READ_EXECUTE, true);

        // Sets the sticky bit on stickyBitDir - now removing file kv1.txt from stickyBitDir by
        // unprivileged user will result in a DFS error.
        fs.setPermission(stickyBitDir, fsPermission);

        FileStatus[] files = fs.listStatus(stickyBitDir);

        // Connecting to HS2 as foo.
        Connection hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL(), "foo", "bar");
        Statement stmt = hs2Conn.createStatement();

        String tableName = "stickyBitTable";

        stmt.execute("drop table if exists " + tableName);
        stmt.execute("create table " + tableName + " (foo int, bar string)");

        try {
            // This statement will attempt to move kv1.txt out of stickyBitDir as user foo.  HS2 is
            // expected to return 20009.
            stmt.execute("LOAD DATA INPATH '" + stickyBitDir.toUri().getPath() + "/kv1.txt' "
                    + "OVERWRITE INTO TABLE " + tableName);
        } catch (Exception e) {
            if (e instanceof SQLException) {
                SQLException se = (SQLException) e;
                Assert.assertTrue("Unexpected error code", se.getMessage().contains("and load data is also not ran as"));
//                Assert.assertEquals("Unexpected error code", 40000, se.getErrorCode());
                System.out.println(String.format("Error Message: %s", se.getMessage()));
            } else
                throw e;
        }

        stmt.execute("drop table if exists " + tableName);

        stmt.close();
        hs2Conn.close();
    }
}
