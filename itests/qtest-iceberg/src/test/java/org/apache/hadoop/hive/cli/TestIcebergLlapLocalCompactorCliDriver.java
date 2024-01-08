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
package org.apache.hadoop.hive.cli;

import java.io.File;
import java.util.List;

import org.apache.hadoop.hive.cli.control.CliAdapter;
import org.apache.hadoop.hive.cli.control.CliConfigs;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.txn.compactor.Worker;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import java.util.concurrent.atomic.AtomicBoolean;

@RunWith(Parameterized.class)
public class TestIcebergLlapLocalCompactorCliDriver {

  static CliAdapter adapter = new CliConfigs.IcebergLlapLocalCompactorCliConfig().getCliAdapter();
  private static final AtomicBoolean stop = new AtomicBoolean();
  private static Worker worker;

  @Parameters(name ="{0}")
  public static List<Object[]> getParameters() throws Exception {
    return adapter.getParameters();
  }

  @BeforeClass
  public static void setup() throws Exception {
    worker = new Worker();
    worker.setConf(SessionState.get().getConf());
    stop.set(false);
    worker.init(stop);
    worker.start();
  }

  @AfterClass
  public static void tearDown(){
    stop.set(true);
  }

  @ClassRule
  public static TestRule cliClassRule = adapter.buildClassRule();

  @Rule
  public TestRule cliTestRule = adapter.buildTestRule();

  private String name;
  private File qfile;

  public TestIcebergLlapLocalCompactorCliDriver(String name, File qfile) {
    this.name = name;
    this.qfile = qfile;
  }

  @Test
  public void testCliDriver() throws Exception {
    adapter.runTest(name, qfile);
  }
}
