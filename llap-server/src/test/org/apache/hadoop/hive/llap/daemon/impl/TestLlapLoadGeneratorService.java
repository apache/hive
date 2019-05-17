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
package org.apache.hadoop.hive.llap.daemon.impl;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

/**
 * Test to make sure that the LLAP nodes are able to start with the load generator.
 */
public class TestLlapLoadGeneratorService {
  @Test
  public void testLoadGenerator() throws InterruptedException, UnknownHostException {
    LlapLoadGeneratorService service = new LlapLoadGeneratorService();

    HiveConf conf = new HiveConf();
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_TEST_LOAD_ENABLED, true);
    HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_TEST_LOAD_HOSTNAMES,
        InetAddress.getLocalHost().getHostName() + ",???");
    HiveConf.setFloatVar(conf, HiveConf.ConfVars.HIVE_TEST_LOAD_UTILIZATION, 0.2f);
    HiveConf.setTimeVar(conf, HiveConf.ConfVars.HIVE_TEST_LOAD_INTERVAL, 5, TimeUnit.MILLISECONDS);

    service.init(conf);
    service.start();
    Thread.sleep(10000);
    service.stop();
  }
}
