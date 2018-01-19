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

package org.apache.hive.jdbc.miniHS2;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.service.cli.CLIServiceClient;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.SessionHandle;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestHiveServer2SessionTimeout {

  private static MiniHS2 miniHS2 = null;
  private Map<String, String> confOverlay;

  @BeforeClass
  public static void beforeTest() throws Exception {
    miniHS2 = new MiniHS2(new HiveConf());
  }

  @Before
  public void setUp() throws Exception {
    confOverlay = new HashMap<String, String>();
    confOverlay.put(ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    confOverlay.put(ConfVars.HIVE_SERVER2_SESSION_CHECK_INTERVAL.varname, "3s");
    confOverlay.put(ConfVars.HIVE_SERVER2_IDLE_OPERATION_TIMEOUT.varname, "3s");
    miniHS2.start(confOverlay);
  }

  @After
  public void tearDown() throws Exception {
    miniHS2.stop();
  }

  @Test
  public void testConnection() throws Exception {
    CLIServiceClient serviceClient = miniHS2.getServiceClient();
    SessionHandle sessHandle = serviceClient.openSession("foo", "bar");
    OperationHandle handle = serviceClient.executeStatement(sessHandle, "SELECT 1", confOverlay);
    Thread.sleep(7000);
    try {
      serviceClient.closeOperation(handle);
      fail("Operation should have been closed by timeout!");
    } catch (HiveSQLException e) {
      assertTrue(StringUtils.stringifyException(e),
          e.getMessage().contains("Invalid OperationHandle"));
    }
  }
}
