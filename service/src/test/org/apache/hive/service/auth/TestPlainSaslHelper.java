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
package org.apache.hive.service.auth;

import junit.framework.TestCase;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIService;
import org.apache.hive.service.cli.thrift.ThriftBinaryCLIService;
import org.apache.thrift.TProcessorFactory;

public class TestPlainSaslHelper extends TestCase {

  /**
   * Test setting {@link HiveConf.ConfVars}} config parameter
   *   HIVE_SERVER2_ENABLE_DOAS for unsecure mode
   */
  public void testDoAsSetting(){

    HiveConf hconf = new HiveConf();
    hconf
    .setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    assertTrue("default value of hive server2 doAs should be true",
        hconf.getBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS));


    CLIService cliService = new CLIService(null);
    cliService.init(hconf);
    ThriftCLIService tcliService = new ThriftBinaryCLIService(cliService, null);
    tcliService.init(hconf);
    TProcessorFactory procFactory = PlainSaslHelper.getPlainProcessorFactory(tcliService);
    assertEquals("doAs enabled processor for unsecure mode",
        procFactory.getProcessor(null).getClass(), TSetIpAddressProcessor.class);
  }
}
