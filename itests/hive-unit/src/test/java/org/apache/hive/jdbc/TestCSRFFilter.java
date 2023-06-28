/*
 * Licensed to the Apache Software Foundation (ASF) under one 
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package test.java.org.apache.hive.jdbc;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.jdbc.CsrfHttpRequestInterceptor;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.sql.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * TestCSRFFilter test.
 */
@RunWith(Parameterized.class)
public class TestCSRFFilter {

  private static MiniHS2 miniHS2 = null;
  private static String dataFileDir;
  private static Path kvDataFilePath;
  private static final String tmpDir = System.getProperty("test.tmp.dir");

  private Connection hs2Conn = null;

  @Parameterized.Parameter
  public String transportMode =  null;

  @Parameterized.Parameters(name = "{index}: tranportMode={0}")
  public static Collection<Object[]> transportModes() {
    return Arrays.asList(new Object[][]{{MiniHS2.HS2_ALL_MODE}, {MiniHS2.HS2_HTTP_MODE}});
  }


  @BeforeClass
  public static void beforeClass() throws IOException {
    MiniHS2.cleanupLocalDir();
  }

  @AfterClass
  public static void afterClass() throws IOException {
    MiniHS2.cleanupLocalDir();
  }

  // This is not modeled as a @Before, because it needs to be parameterized per-test.
  // If there is a better way to do this, we should do it.
  private void initHS2(boolean enableCSRFFilter) throws Exception {
    Class.forName(MiniHS2.getJdbcDriverName());
    HiveConf conf = new HiveConf();
    conf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    miniHS2 = new MiniHS2.Builder().withConf(conf).cleanupLocalDirOnStartup(false).build();
    dataFileDir = conf.get("test.data.files").replace('\\', '/').replace("c:", "");
    kvDataFilePath = new Path(dataFileDir, "kv1.txt");
    Map<String,String> confOverlay = new HashMap<String, String>();
    confOverlay.put(ConfVars.HIVE_SERVER2_CSRF_FILTER_ENABLED.varname, String.valueOf(enableCSRFFilter));
    confOverlay.put(ConfVars.HIVE_SERVER2_TRANSPORT_MODE.varname, transportMode);
    miniHS2.start(confOverlay);
  }

  private Connection getConnection(String jdbcURL, String user, String pwd) throws SQLException {
    Connection conn = DriverManager.getConnection(jdbcURL, user, pwd);
    conn.createStatement().execute("set hive.support.concurrency = false");
    return conn;
  }

  @After
  public void tearDownHS2() throws Exception {
    if (hs2Conn != null){
      hs2Conn.close();
      hs2Conn = null;
    }
    if ((miniHS2!= null) && miniHS2.isStarted()) {
      miniHS2.stop();
      miniHS2 = null;
    }
  }

  @Test
  public void testFilterDisabledNoInjection() throws Exception {
    // filter disabled, injection disabled, exception not expected
    runTest(false,false);
  }

  @Test
  public void testFilterDisabledWithInjection() throws Exception {
    // filter disabled, injection enabled, exception not expected
    runTest(false,true);
  }

  @Test
  public void testFilterEnabledWithInjection() throws Exception {
    // filter enabled, injection enabled, exception not expected
    runTest(true,true);
  }

  @Test
  public void testFilterEnabledNoInjection() throws Exception {
    // filter enabled, injection disabled, exception expected
    runTest(true,false);
  }

  private void runTest(boolean isFilterEnabled, boolean isInjectionEnabled) throws Exception {
    // Exception is expected only if filter is enabled and injection is disabled
    boolean exceptionExpected = isFilterEnabled && (!isInjectionEnabled);
    initHS2(isFilterEnabled);
    CsrfHttpRequestInterceptor.setInjectHeader(isInjectionEnabled);
    Exception e = null;
    try {
      runBasicCommands();
    } catch (Exception thrown) {
      e = thrown;
    }
    if (exceptionExpected){
      assertNotNull(e);
    } else {
      assertEquals(null,e);
    }
  }


  private void runBasicCommands() throws Exception {
    hs2Conn = getConnection(miniHS2.getHttpJdbcURL(), System.getProperty("user.name"), "bar");
    String tableName = "testTab1";
    Statement stmt = hs2Conn.createStatement();

    // create table
    stmt.execute("DROP TABLE IF EXISTS " + tableName);
    stmt.execute("CREATE TABLE " + tableName
        + " (under_col INT COMMENT 'the under column', value STRING) COMMENT ' test table'");

    // load data
    stmt.execute("load data local inpath '"
        + kvDataFilePath.toString() + "' into table " + tableName);

    ResultSet res = stmt.executeQuery("SELECT * FROM " + tableName);
    assertTrue(res.next());
    assertEquals("val_238", res.getString(2));
    res.close();
    stmt.close();
  }

}
