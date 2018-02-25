/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.LlapBaseInputFormat;
import org.apache.hadoop.hive.metastore.api.WMTrigger;
import org.apache.hadoop.hive.ql.wm.Trigger;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.jdbc.miniHS2.MiniHS2.MiniClusterType;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public abstract class AbstractJdbcTriggersTest {
  protected static MiniHS2 miniHS2 = null;
  protected static String dataFileDir;
  static Path kvDataFilePath;
  protected static String tableName = "testtab1";

  protected static HiveConf conf = null;
  protected Connection hs2Conn = null;

  @BeforeClass
  public static void beforeTest() throws Exception {
    Class.forName(MiniHS2.getJdbcDriverName());

    String confDir = "../../data/conf/llap/";
    HiveConf.setHiveSiteLocation(new URL("file://" + new File(confDir).toURI().getPath() + "/hive-site.xml"));
    System.out.println("Setting hive-site: " + HiveConf.getHiveSiteLocation());

    conf = new HiveConf();
    conf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    conf.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
    conf.setVar(ConfVars.HIVE_SERVER2_TEZ_DEFAULT_QUEUES, "default");
    conf.setTimeVar(ConfVars.HIVE_TRIGGER_VALIDATION_INTERVAL, 100, TimeUnit.MILLISECONDS);
    conf.setBoolVar(ConfVars.HIVE_SERVER2_TEZ_INITIALIZE_DEFAULT_SESSIONS, true);
    conf.setBoolVar(ConfVars.TEZ_EXEC_SUMMARY, true);
    conf.setBoolVar(ConfVars.HIVE_STRICT_CHECKS_CARTESIAN, false);
    conf.setVar(ConfVars.LLAP_IO_MEMORY_MODE, "none");

    conf.addResource(new URL("file://" + new File(confDir).toURI().getPath()
      + "/tez-site.xml"));

    miniHS2 = new MiniHS2(conf, MiniClusterType.LLAP);
    dataFileDir = conf.get("test.data.files").replace('\\', '/').replace("c:", "");
    kvDataFilePath = new Path(dataFileDir, "kv1.txt");

    Map<String, String> confOverlay = new HashMap<>();
    miniHS2.start(confOverlay);
    miniHS2.getDFS().getFileSystem().mkdirs(new Path("/apps_staging_dir/anonymous"));
  }

  @Before
  public void setUp() throws Exception {
    hs2Conn = TestJdbcWithMiniLlap.getConnection(miniHS2.getJdbcURL(), System.getProperty("user.name"), "bar");
  }

  @After
  public void tearDown() throws Exception {
    LlapBaseInputFormat.closeAll();
    hs2Conn.close();
  }

  @AfterClass
  public static void afterTest() throws Exception {
    if (miniHS2.isStarted()) {
      miniHS2.stop();
    }
  }

  private void createSleepUDF() throws SQLException {
    String udfName = TestJdbcWithMiniHS2.SleepMsUDF.class.getName();
    Connection con = hs2Conn;
    Statement stmt = con.createStatement();
    stmt.execute("create temporary function sleep as '" + udfName + "'");
    stmt.close();
  }

  void runQueryWithTrigger(final String query, final List<String> setCmds,
    final String expect) throws Exception {
    runQueryWithTrigger(query, setCmds, expect, null);
  }

  void runQueryWithTrigger(final String query, final List<String> setCmds,
    final String expect, final List<String> errCaptureExpect)
    throws Exception {

    Connection con = hs2Conn;
    TestJdbcWithMiniLlap.createTestTable(con, null, tableName, kvDataFilePath.toString());
    createSleepUDF();

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    System.setErr(new PrintStream(baos)); // capture stderr
    final Statement selStmt = con.createStatement();
    final Throwable[] throwable = new Throwable[1];
    try {
      Thread queryThread = new Thread(() -> {
        try {
          if (setCmds != null) {
            for (String setCmd : setCmds) {
              selStmt.execute(setCmd);
            }
          }
          selStmt.execute(query);
        } catch (SQLException e) {
          throwable[0] = e;
        }
      });
      queryThread.start();

      queryThread.join();
      selStmt.close();

      if (expect == null) {
        assertNull("Expected query to succeed", throwable[0]);
      } else {
        assertNotNull("Expected non-null throwable", throwable[0]);
        assertEquals(SQLException.class, throwable[0].getClass());
        assertTrue(expect + " is not contained in " + throwable[0].getMessage(),
          throwable[0].getMessage().contains(expect));
      }

      if (errCaptureExpect != null && !errCaptureExpect.isEmpty()) {
        // failure hooks are run after HiveStatement is closed. wait sometime for failure hook to execute
        String stdErrStr = "";
        while (!stdErrStr.contains(errCaptureExpect.get(0))) {
          baos.flush();
          stdErrStr = baos.toString();
          Thread.sleep(500);
        }
        for (String errExpect : errCaptureExpect) {
          assertTrue("'" + errExpect + "' expected in STDERR capture, but not found.", stdErrStr.contains(errExpect));
        }
      }
    } finally {
      baos.close();
    }

  }

  abstract void setupTriggers(final List<Trigger> triggers) throws Exception;

  List<String> getConfigs(String... more) {
    List<String> setCmds = new ArrayList<>();
    setCmds.add("set hive.exec.dynamic.partition.mode=nonstrict");
    setCmds.add("set mapred.min.split.size=100");
    setCmds.add("set mapred.max.split.size=100");
    setCmds.add("set tez.grouping.min-size=100");
    setCmds.add("set tez.grouping.max-size=100");
    if (more != null) {
      setCmds.addAll(Arrays.asList(more));
    }
    return setCmds;
  }

  WMTrigger wmTriggerFromTrigger(Trigger trigger) {
    WMTrigger result = new WMTrigger("rp", trigger.getName());
    result.setTriggerExpression(trigger.getExpression().toString());
    result.setActionExpression(trigger.getAction().toString());
    return result;
  }
}