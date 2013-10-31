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

package org.apache.hive.service.server;

import static org.junit.Assert.fail;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QTestUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * TestHiveServer2Concurrency.
 *
 */
public class TestHiveServer2Concurrency {

  private static QTestUtil.QTestSetup miniZKCluster = null;
  private static HiveServer2 hiveServer2;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    HiveConf hiveConf = new HiveConf();

    miniZKCluster = new QTestUtil.QTestSetup();
    miniZKCluster.preTest(hiveConf);

    hiveServer2 = new HiveServer2();
    hiveServer2.init(hiveConf);
    hiveServer2.start();
    Thread.sleep(5000);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (hiveServer2 != null) {
      hiveServer2.stop();
    }
    if (miniZKCluster != null) {
      try {
        miniZKCluster.tearDown();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  class QueryRunner implements Runnable {

    @Override
    public void run() {
      // TODO Auto-generated method stub

    }

  }


  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void test() {
    fail("Not yet implemented");
  }

}
