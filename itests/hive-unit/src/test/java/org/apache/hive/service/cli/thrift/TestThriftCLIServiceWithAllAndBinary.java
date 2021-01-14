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

package org.apache.hive.service.cli.thrift;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.Service;
import org.apache.hive.service.auth.HiveAuthConstants;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.junit.Assert.assertNotNull;

/**
 * TestThriftHttpCLIService.
 * This tests ThriftCLIService started in http mode.
 */

public class TestThriftCLIServiceWithAllAndBinary extends ThriftCLIServiceTest {

  private static String transportMode = "all";

  /**
   * @throws Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Set up the base class
    ThriftCLIServiceTest.setUpBeforeClass();

    assertNotNull(port);
    assertNotNull(hiveServer2);
    assertNotNull(hiveConf);

    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
    hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST, host);
    hiveConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT, port);
    hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION, HiveAuthConstants.AuthTypes.NONE.toString());
    hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_TRANSPORT_MODE, transportMode);

    startHiveServer2WithConf(hiveConf);

    client = getHttpServiceClientInternal();
  }

  /**
   * @throws Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    ThriftCLIServiceTest.tearDownAfterClass();
  }
  static ThriftCLIServiceClient getHttpServiceClientInternal() {
    for (Service service : hiveServer2.getServices()) {
      if (service instanceof ThriftBinaryCLIService) {
        return new ThriftCLIServiceClient((ThriftBinaryCLIService) service);
      }
      if (service instanceof ThriftHttpCLIService) {
        continue;
      }
    }
    throw new IllegalStateException("HiveServer2 not running Thrift service");
  }
}
