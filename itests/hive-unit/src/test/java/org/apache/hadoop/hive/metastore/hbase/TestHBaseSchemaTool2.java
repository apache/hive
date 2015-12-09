/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.hbase;

import org.apache.commons.lang.StringUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

/**
 * This is in a separate class because install tests shouldn't set up the metastore first.
 */
public class TestHBaseSchemaTool2 extends HBaseIntegrationTests {

  private String lsep = System.getProperty("line.separator");

  @BeforeClass
  public static void startup() throws Exception {
    HBaseIntegrationTests.startMiniCluster();
  }

  @AfterClass
  public static void shutdown() throws Exception {
    HBaseIntegrationTests.shutdownMiniCluster();
  }

  @Test
  public void install() {
    ByteArrayOutputStream outStr = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(outStr);
    ByteArrayOutputStream errStr = new ByteArrayOutputStream();
    PrintStream err = new PrintStream(errStr);

    HBaseSchemaTool tool = new HBaseSchemaTool();
    tool.install(conf, err);
    tool.go(true, null, null, null, conf, out, err);
    Assert.assertEquals(StringUtils.join(HBaseReadWrite.tableNames, lsep) + lsep,
        outStr.toString());
  }
}
