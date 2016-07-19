/**
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
package org.apache.hive.service.cli.session;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.operation.OperationManager;
import org.apache.hive.service.cli.thrift.ThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestSessionGlobalInitFile extends TestCase {

  private FakeEmbeddedThriftBinaryCLIService service;
  private ThriftCLIServiceClient client;
  private File initFile;
  private String tmpDir;
  private HiveConf hiveConf;

  /**
   * This class is almost the same as EmbeddedThriftBinaryCLIService,
   * except its constructor having a HiveConf param for test usage.
   */
  private class FakeEmbeddedThriftBinaryCLIService extends ThriftBinaryCLIService {
    public FakeEmbeddedThriftBinaryCLIService(HiveConf hiveConf) {
      super(new CLIService(null), null);
      isEmbedded = true;
      cliService.init(hiveConf);
      cliService.start();
    }

    public CLIService getService() {
      return cliService;
    }
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();

    // create and put .hiverc sample file to default directory
    initFile = File.createTempFile("test", "hive");
    tmpDir =
        initFile.getParentFile().getAbsoluteFile() + File.separator
            + "TestSessionGlobalInitFile";
    initFile.delete();
    FileUtils.deleteDirectory(new File(tmpDir));

    initFile =
        new File(tmpDir + File.separator + SessionManager.HIVERCFILE);
    initFile.getParentFile().mkdirs();
    initFile.createNewFile();

    String[] fileContent =
        new String[] { "-- global init hive file for test", "set a=1;",
            "set hiveconf:b=1;", "set hivevar:c=1;", "set d\\", "      =1;",
            "add jar " + initFile.getAbsolutePath() };
    FileUtils.writeLines(initFile, Arrays.asList(fileContent));

    // set up service and client
    hiveConf = new HiveConf();
    hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_GLOBAL_INIT_FILE_LOCATION,
        initFile.getParentFile().getAbsolutePath());
    hiveConf
    .setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    service = new FakeEmbeddedThriftBinaryCLIService(hiveConf);
    service.init(new HiveConf());
    client = new ThriftCLIServiceClient(service);
  }

  @After
  public void tearDown() throws Exception {
    // restore
    FileUtils.deleteDirectory(new File(tmpDir));
  }

  @Test
  public void testSessionGlobalInitFile() throws Exception {
    File tmpInitFile = new File(initFile.getParent(), "hiverc");
    Assert.assertTrue("Failed to rename " + initFile + " to " + tmpInitFile,
      initFile.renameTo(tmpInitFile));
    initFile = tmpInitFile;
    hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_GLOBAL_INIT_FILE_LOCATION,
        initFile.getAbsolutePath());
    doTestSessionGlobalInitFile();
  }

  @Test
  public void testSessionGlobalInitDir() throws Exception {
    doTestSessionGlobalInitFile();
  }

  /**
   * create session, and fetch the property set in global init file. Test if
   * the global init file .hiverc is loaded correctly by checking the expected
   * setting property.
   */
  private void doTestSessionGlobalInitFile() throws Exception {

    OperationManager operationManager = service.getService().getSessionManager()
        .getOperationManager();
    SessionHandle sessionHandle = client.openSession(null, null, null);

    // ensure there is no operation related object leak
    Assert.assertEquals("Verifying all operations used for init file are closed",
        0, operationManager.getOperations().size());

    verifyInitProperty("a", "1", sessionHandle);
    verifyInitProperty("b", "1", sessionHandle);
    verifyInitProperty("c", "1", sessionHandle);
    verifyInitProperty("hivevar:c", "1", sessionHandle);
    verifyInitProperty("d", "1", sessionHandle);

    /**
     * TODO: client.executeStatement do not support listing resources command
     * (beeline> list jar)
     */
    // Assert.assertEquals("expected uri", api.getAddedResource("jar"));

    Assert.assertEquals("Verifying all operations used for checks are closed",
        0, operationManager.getOperations().size());
    client.closeSession(sessionHandle);

  }

  @Test
  public void testSessionGlobalInitFileWithUser() throws Exception {
    //Test when the session is opened by a user. (HiveSessionImplwithUGI)
    SessionHandle sessionHandle = client.openSession("hive", "password", null);
    verifyInitProperty("a", "1", sessionHandle);
    client.closeSession(sessionHandle);
  }

  @Test
  public void testSessionGlobalInitFileAndConfOverlay() throws Exception {
    // Test if the user session specific conf overlaying global init conf.
    Map<String, String> confOverlay = new HashMap<String, String>();
    confOverlay.put("a", "2");
    confOverlay.put("set:hiveconf:b", "2");
    confOverlay.put("set:hivevar:c", "2");

    SessionHandle sessionHandle = client.openSession(null, null, confOverlay);
    verifyInitProperty("a", "2", sessionHandle);
    verifyInitProperty("b", "2", sessionHandle);
    verifyInitProperty("c", "2", sessionHandle);
    client.closeSession(sessionHandle);

    sessionHandle = client.openSession("hive", "password", confOverlay);
    verifyInitProperty("a", "2", sessionHandle);
    client.closeSession(sessionHandle);
  }

  private void verifyInitProperty(String key, String value,
      SessionHandle sessionHandle) throws Exception {
    OperationHandle operationHandle =
        client.executeStatement(sessionHandle, "set " + key, null);
    RowSet rowSet = client.fetchResults(operationHandle);
    Assert.assertEquals(1, rowSet.numRows());
    // we know rowSet has only one element
    Assert.assertEquals(key + "=" + value, rowSet.iterator().next()[0]);
    client.closeOperation(operationHandle);
  }
}
