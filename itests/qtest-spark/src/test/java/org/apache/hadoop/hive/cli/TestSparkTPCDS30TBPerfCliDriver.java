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

/**
 * The test exists only as a replacement of the old SparkPerfDriver. 
 * 
 * The test was never executed since Spark tests are broken at the time being and there was relevant discussion
 * to entirely remove supoort for Spark. Currently, the test script calle by maven cannot locate the Spark artifacts:
 * http://d3jw87u4immizc.cloudfront.net/spark-tarball/spark-2.4.5-bin-hadoop3-beta1-without-hive.tgz
 */
@RunWith(Parameterized.class)
public class TestSparkTPCDS30TBPerfCliDriver {

  static CliAdapter adapter = new CliConfigs.SparkTPCDS30TBCliConfig().getCliAdapter();

  @Parameters(name = "{0}")
  public static List<Object[]> getParameters() throws Exception {
    return adapter.getParameters();
  }

  @ClassRule
  public static TestRule cliClassRule = adapter.buildClassRule();

  /**
   * Rule for calling only {@link CliAdapter#setUp()} and {@link CliAdapter#tearDown()} before/after running each test.
   *
   * At the moment of writing this class the rule is mostly necessary for calling {@link CliAdapter#tearDown()} to avoid
   * state from one test pass to other (e.g., disabling one test should not disable subsequent ones).
   *
   * {@link CliAdapter#buildTestRule()} cannot not used since it is doing more than necessary for this test case. For
   * instance, we do not want to create and destroy the metastore after each query.
   */
  @Rule
  public TestRule cliTestRule = adapter.buildTestRule();

  private final String name;
  private final File qfile;

  public TestSparkTPCDS30TBPerfCliDriver(String name, File qfile) {
    this.name = name;
    this.qfile = qfile;
  }

  @Test
  public void testCliDriver() throws Exception {
    adapter.runTest(name, qfile);
  }

}
