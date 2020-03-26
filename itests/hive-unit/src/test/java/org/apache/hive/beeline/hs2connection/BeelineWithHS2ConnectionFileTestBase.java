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
package org.apache.hive.beeline.hs2connection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.beeline.BeeLine;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.service.cli.CLIServiceClient;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.SessionHandle;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * BeelineWithHS2ConnectionFileTestBase test.
 */
@RunWith(Parameterized.class)
public abstract class BeelineWithHS2ConnectionFileTestBase {
  protected MiniHS2 miniHS2;
  protected HiveConf hiveConf = new HiveConf();
  protected final String tableName = "testBeelineTable";
  protected String dataFileDir = hiveConf.get("test.data.files");
  protected static final String LOCALHOST_KEY_STORE_NAME = "keystore.jks";
  protected static final String TRUST_STORE_NAME = "truststore.jks";
  protected static final String KEY_STORE_TRUST_STORE_PASSWORD = "HiveJdbc";
  protected static final String HS2_HTTP_MODE = "http";
  protected static final String HS2_HTTP_ENDPOINT = "cliservice";
  private final String fileLocation =
      System.getProperty("java.io.tmpdir") + "testHs2ConnectionConfig.xml";
  protected static final String JAVA_TRUST_STORE_PROP = "javax.net.ssl.trustStore";
  protected static final String JAVA_TRUST_STORE_PASS_PROP = "javax.net.ssl.trustStorePassword";

  protected Map<String, String> confOverlay = new HashMap<>();

  @Parameterized.Parameter
  public String transportMode =  null;


  protected class TestBeeLine extends BeeLine {
    UserHS2ConnectionFileParser testHs2ConfigFileManager;
    ByteArrayOutputStream os;

    TestBeeLine(List<String> defaultHS2ConnectionFiles) {
      testHs2ConfigFileManager = new UserHS2ConnectionFileParser(defaultHS2ConnectionFiles);
      os = new ByteArrayOutputStream();
      PrintStream beelineOutputStream = new PrintStream(os);
      setOutputStream(beelineOutputStream);
      setErrorStream(beelineOutputStream);
    }

    TestBeeLine() {
      testHs2ConfigFileManager = new UserHS2ConnectionFileParser(null);
      os = new ByteArrayOutputStream();
      PrintStream beelineOutputStream = new PrintStream(os);
      setOutputStream(beelineOutputStream);
      setErrorStream(beelineOutputStream);
    }

    public String getOutput() throws UnsupportedEncodingException {
      return os.toString("UTF8");
    }

    @Override
    public UserHS2ConnectionFileParser getUserHS2ConnFileParser() {
      return testHs2ConfigFileManager;
    }

    @Override
    public HS2ConnectionFileParser getHiveSiteHS2ConnectionFileParser() {
      HiveSiteHS2ConnectionFileParser ret = new HiveSiteHS2ConnectionFileParser();
      ret.setHiveConf(miniHS2.getHiveConf());
      return ret;
    }
  }

  /*
   * Wrapper class to write a HS2ConnectionConfig file
   */
  protected class Hs2ConnectionXmlConfigFileWriter {
    private final PrintWriter writer;
    private final File file;
    private final Configuration conf;

    protected Hs2ConnectionXmlConfigFileWriter() throws IOException {
      file = new File(fileLocation);
      conf = new Configuration(false);
      try {
        if (file.exists()) {
          file.delete();
        }
        file.createNewFile();
        writer = new PrintWriter(file.getAbsolutePath(), "UTF-8");
      } finally {
        file.deleteOnExit();
      }
    }

    protected void writeProperty(String key, String value) {
      conf.set(key, value);
    }

    protected String path() {
      return file.getAbsolutePath();
    }

    protected void close() throws IOException {
      try {
        conf.writeXml(writer);
      } finally {
        writer.close();
      }
    }
  }

  @BeforeClass
  public static void beforeTest() throws Exception {
    MiniHS2.cleanupLocalDir();
    Class.forName(MiniHS2.getJdbcDriverName());
  }

  @AfterClass
  public static void afterClass() throws IOException {
    MiniHS2.cleanupLocalDir();
  }

  @Before
  public void before() throws Exception {
    DriverManager.setLoginTimeout(0);
    if (!System.getProperty("test.data.files", "").isEmpty()) {
      dataFileDir = System.getProperty("test.data.files");
    }
    dataFileDir = dataFileDir.replace('\\', '/').replace("c:", "");
    hiveConf = new HiveConf();
    miniHS2 = getNewMiniHS2();
    confOverlay = new HashMap<String, String>();
    confOverlay.put(ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    confOverlay.put(ConfVars.HIVE_SERVER2_TRANSPORT_MODE.varname, transportMode);
    confOverlay.put(ConfVars.HIVE_SERVER2_USE_SSL.varname, "false");
  }

  protected MiniHS2 getNewMiniHS2() throws Exception {
    return new MiniHS2.Builder().withConf(hiveConf).cleanupLocalDirOnStartup(false).build();
  }

  @After
  public void tearDown() throws Exception {
    if (miniHS2 != null && miniHS2.isStarted()) {
      miniHS2.stop();
    }
    miniHS2 = null;
    System.clearProperty(JAVA_TRUST_STORE_PROP);
    System.clearProperty(JAVA_TRUST_STORE_PASS_PROP);
  }

  protected void createTable() throws HiveSQLException {
    CLIServiceClient serviceClient = miniHS2.getServiceClient();
    SessionHandle sessHandle = serviceClient.openSession("foo", "bar");
    serviceClient.executeStatement(sessHandle, "DROP TABLE IF EXISTS " + tableName, confOverlay);
    serviceClient.executeStatement(sessHandle, "CREATE TABLE " + tableName + " (id INT)",
        confOverlay);
    OperationHandle opHandle =
        serviceClient.executeStatement(sessHandle, "SHOW TABLES", confOverlay);
    RowSet rowSet = serviceClient.fetchResults(opHandle);
    assertFalse(rowSet.numRows() == 0);
  }

  protected void assertBeelineOutputContains(String path, String[] beelineArgs,
      String expectedOutput) throws Exception {
    BeelineResult res = getBeelineOutput(path, beelineArgs);
    assertEquals(0, res.exitCode);
    Assert.assertNotNull(res.output);
    Assert.assertTrue("Output " + res.output + " does not contain " + expectedOutput,
        res.output.toLowerCase().contains(expectedOutput.toLowerCase()));
  }

  static class BeelineResult {

    public final String output;
    public final int exitCode;

    public BeelineResult(String output, int exitCode) {
      this.output = output;
      this.exitCode = exitCode;
    }

  }

  protected BeelineResult getBeelineOutput(String path, String[] beelineArgs) throws Exception {
    TestBeeLine beeLine = null;
    try {
      if(path != null) {
        List<String> testLocations = new ArrayList<>();
        testLocations.add(path);
        beeLine = new TestBeeLine(testLocations);
      } else {
        beeLine = new TestBeeLine();
      }
      int exitCode = beeLine.begin(beelineArgs, null);
      String output = beeLine.getOutput();
      System.out.println(output);
      return new BeelineResult(output, exitCode);
    } finally {
      if (beeLine != null) {
        beeLine.close();
      }
    }
  }
}
