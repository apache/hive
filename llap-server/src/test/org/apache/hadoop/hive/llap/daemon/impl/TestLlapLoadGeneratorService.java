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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

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
  public void testLoadGeneratorStops() throws InterruptedException, UnknownHostException {
    LlapLoadGeneratorService service = new LlapLoadGeneratorService();

    HiveConf conf = new HiveConf();
    HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_TEST_LOAD_HOSTNAMES,
        InetAddress.getLocalHost().getHostName() + ",???");
    HiveConf.setFloatVar(conf, HiveConf.ConfVars.HIVE_TEST_LOAD_UTILIZATION, 0.5f);
    HiveConf.setTimeVar(conf, HiveConf.ConfVars.HIVE_TEST_LOAD_INTERVAL, 5, TimeUnit.MILLISECONDS);

    service.init(conf);
    service.start();
    assertEquals("The number of threads is not correct",
        Runtime.getRuntime().availableProcessors(), service.threads.length);
    for(int i = 0; i < service.threads.length; i++) {
      assertTrue("The thread [" + i + "] should be alive", service.threads[i].isAlive());
    }
    service.stop();
    Thread.sleep(1000);
    for(int i = 0; i < service.threads.length; i++) {
      Thread.State state = service.threads[i].getState();
      assertFalse("The thread [" + i + "] should be terminated", service.threads[i].isAlive());
    }
  }

  @Test(expected = RuntimeException.class)
  public void testLoadGeneratorFails() throws InterruptedException, UnknownHostException {
    LlapLoadGeneratorService service = new LlapLoadGeneratorService();

    HiveConf conf = new HiveConf();
    HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_TEST_LOAD_HOSTNAMES,
        InetAddress.getLocalHost().getHostName() + ",???");
    HiveConf.setFloatVar(conf, HiveConf.ConfVars.HIVE_TEST_LOAD_UTILIZATION, 1.2f);
    HiveConf.setTimeVar(conf, HiveConf.ConfVars.HIVE_TEST_LOAD_INTERVAL, 5, TimeUnit.MILLISECONDS);

    service.init(conf);
    service.start();
    service.stop();
  }
}
