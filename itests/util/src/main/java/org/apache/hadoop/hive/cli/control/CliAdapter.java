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
package org.apache.hadoop.hive.cli.control;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * This class adapts old vm test-executors to be executed in multiple instances
 */
public abstract class CliAdapter {

  protected final AbstractCliConfig cliConfig;

  public CliAdapter(AbstractCliConfig cliConfig) {
    this.cliConfig = cliConfig;
  }

  public final List<Object[]> getParameters() throws Exception {
    Set<File> f = cliConfig.getQueryFiles();
    List<Object[]> ret = new ArrayList<>();

    for (File file : f) {
      String label = file.getName().replaceAll("\\.[^\\.]+$", "");
      ret.add(new Object[] { label, file });
    }
    return ret;
  }

  abstract public void beforeClass() throws Exception;

  // HIVE-14444 pending rename: before
  abstract public void setUp();

  // HIVE-14444 pending rename: after
  abstract public void tearDown();

  // HIVE-14444 pending rename: afterClass
  abstract public void shutdown() throws Exception;

  abstract public void runTest(String name, String name2, String absolutePath) throws Exception;

  public final TestRule buildClassRule() {
    return new TestRule() {
      @Override
      public Statement apply(final Statement base, Description description) {
        return new Statement() {
          @Override
          public void evaluate() throws Throwable {
            CliAdapter.this.beforeClass();
            try {
              base.evaluate();
            } finally {
              CliAdapter.this.shutdown();
            }
          }
        };
      }
    };
  }

  public final TestRule buildTestRule() {
    return new TestRule() {
      @Override
      public Statement apply(final Statement base, Description description) {
        return new Statement() {
          @Override
          public void evaluate() throws Throwable {
            CliAdapter.this.setUp();
            try {
              base.evaluate();
            } finally {
              CliAdapter.this.tearDown();
            }
          }
        };
      }
    };
  }

  // HIVE-14444: pending refactor to push File forward
  public final void runTest(String name, File qfile) throws Exception {
    runTest(name, qfile.getName(), qfile.getAbsolutePath());
  }

}
