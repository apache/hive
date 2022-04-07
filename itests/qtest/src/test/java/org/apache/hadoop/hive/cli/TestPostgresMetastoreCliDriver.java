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

import org.apache.hadoop.hive.cli.control.CliAdapter;
import org.apache.hadoop.hive.cli.control.CliConfigs;
import org.apache.hadoop.hive.cli.control.SplitSupport;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.model.Statement;

import java.io.File;
import java.util.List;

@RunWith(Parameterized.class)
public class TestPostgresMetastoreCliDriver {

  static CliAdapter adapter = new CliConfigs.PostgresMetastoreCliConfig().getCliAdapter();

  private static final int N_SPLITS = 32;

  @Parameters(name = "{0}")
  public static List<Object[]> getParameters() throws Exception {
    return SplitSupport.process(adapter.getParameters(), TestPostgresMetastoreCliDriver.class, N_SPLITS);
  }

  @ClassRule
  public static TestRule cliClassRule = adapter.buildClassRule();

  @Rule
  public TestRule cliTestRule = (statement, description) -> new Statement() {
    @Override
    public void evaluate() throws Throwable {
      adapter.setUp();
      try {
        statement.evaluate();
      } finally {
        adapter.tearDown();
      }
    }
  };

  private final String name;
  private final File qfile;

  public TestPostgresMetastoreCliDriver(String name, File qfile) {
    this.name = name;
    this.qfile = qfile;
  }

  @Test
  public void testCliDriver() throws Exception {
    adapter.runTest(name, qfile);
  }
}
