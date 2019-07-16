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
package org.apache.hadoop.hive.upgrade.acid;

import static org.apache.hadoop.hive.upgrade.acid.PreUpgradeTool.createCommandLineOptions;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import org.apache.commons.cli.GnuParser;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TestRunOptions {

  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  @Test
  public void testTablePoolSizeIs5WhenSpecified() throws Exception {
    String[] args = {"-tablePoolSize", "5"};
    RunOptions runOptions = RunOptions.fromCommandLine(new GnuParser().parse(createCommandLineOptions(), args));
    assertThat(runOptions.getTablePoolSize(), is(5));
  }

  @Test
  public void testExceptionIsThrownWhenTablePoolSizeIsNotANumber() throws Exception {
    expectedEx.expect(IllegalArgumentException.class);
    expectedEx.expectMessage("Please specify a positive integer option value for tablePoolSize");

    String[] args = {"-tablePoolSize", "notANumber"};
    RunOptions.fromCommandLine(new GnuParser().parse(createCommandLineOptions(), args));
  }

  @Test
  public void testExceptionIsThrownWhenTablePoolSizeIsLessThan1() throws Exception {
    expectedEx.expect(IllegalArgumentException.class);
    expectedEx.expectMessage("Please specify a positive integer option value for tablePoolSize");

    String[] args = {"-tablePoolSize", "0"};
    RunOptions.fromCommandLine(new GnuParser().parse(createCommandLineOptions(), args));
  }

  @Test
  public void testExceptionIsThrownWhenTablePoolSizeIsNotInteger() throws Exception {
    expectedEx.expect(IllegalArgumentException.class);
    expectedEx.expectMessage("Please specify a positive integer option value for tablePoolSize");

    String[] args = {"-tablePoolSize", "0.5"};
    RunOptions.fromCommandLine(new GnuParser().parse(createCommandLineOptions(), args));
  }
}
