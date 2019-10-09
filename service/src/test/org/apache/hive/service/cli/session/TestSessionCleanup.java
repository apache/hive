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

package org.apache.hive.service.cli.session;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;



import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.thrift.EmbeddedThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;
import org.junit.Assert;
import org.junit.Test;

/**
 * TestSessionCleanup.
 */
public class TestSessionCleanup {
  // Create subclass of EmbeddedThriftBinaryCLIService, just so we can get an accessor to the CLIService.
  // Needed for access to the OperationManager.
  private class MyEmbeddedThriftBinaryCLIService extends EmbeddedThriftBinaryCLIService {
    public MyEmbeddedThriftBinaryCLIService() {
      super();
    }

    public CLIService getCliService() {
      return cliService;
    }
  }

  @Test
  // This is to test session temporary files are cleaned up after HIVE-11768
  public void testTempSessionFileCleanup() throws Exception {
    MyEmbeddedThriftBinaryCLIService service = new MyEmbeddedThriftBinaryCLIService();
    HiveConf hiveConf = new HiveConf();
    hiveConf
        .setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
            "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    service.init(hiveConf);
    ThriftCLIServiceClient client = new ThriftCLIServiceClient(service);

    Set<String> existingPipeoutFiles = new HashSet<String>(Arrays.asList(getPipeoutFiles()));
    SessionHandle sessionHandle = client.openSession("user1", "foobar",
          Collections.<String, String>emptyMap());
    OperationHandle opHandle1 = client.executeStatement(sessionHandle, "set a=b", null);
    String queryId1 = service.getCliService().getQueryId(opHandle1.toTOperationHandle());
    Assert.assertNotNull(queryId1);
    OperationHandle opHandle2 = client.executeStatement(sessionHandle, "set b=c", null);
    String queryId2 = service.getCliService().getQueryId(opHandle2.toTOperationHandle());
    Assert.assertNotNull(queryId2);
    File operationLogRootDir = new File(
        new HiveConf().getVar(ConfVars.HIVE_SERVER2_LOGGING_OPERATION_LOG_LOCATION));
    Assert.assertNotEquals(operationLogRootDir.list().length, 0);
    client.closeSession(sessionHandle);

    // Check if session files are removed
    Assert.assertEquals(operationLogRootDir.list().length, 0);

    // Check if the pipeout files are removed
    Set<String> finalPipeoutFiles = new HashSet<String>(Arrays.asList(getPipeoutFiles()));
    finalPipeoutFiles.removeAll(existingPipeoutFiles);
    Assert.assertTrue(finalPipeoutFiles.isEmpty());

    // Verify both operationHandles are no longer held by the OperationManager
    Assert.assertEquals(0, service.getCliService().getSessionManager().getOperations().size());

    // Verify both queryIds are no longer held by the OperationManager
    Assert.assertNull(service.getCliService().getSessionManager().getOperationManager().getOperationByQueryId(queryId2));
    Assert.assertNull(service.getCliService().getSessionManager().getOperationManager().getOperationByQueryId(queryId1));
  }

  private String[] getPipeoutFiles() {
    File localScratchDir = new File(
        new HiveConf().getVar(HiveConf.ConfVars.LOCALSCRATCHDIR));
    String[] pipeoutFiles = localScratchDir.list(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        if (name.endsWith("pipeout")) return true;
        return false;
      }
    });
    return pipeoutFiles;
  }
}
