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

import org.apache.hadoop.hive.ql.QTestMetaStoreHandler;
import org.apache.hadoop.hive.ql.QTestUtil;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class adapts old vm test-executors to be executed in multiple instances
 */
public abstract class CliAdapter {

  protected final AbstractCliConfig cliConfig;
  protected QTestMetaStoreHandler metaStoreHandler;
  boolean firstTestNotYetRun = true; // this can protect class/test level logic from each other
  private static final Logger LOG = LoggerFactory.getLogger(CliAdapter.class);

  public CliAdapter(AbstractCliConfig cliConfig) {
    this.cliConfig = cliConfig;
    metaStoreHandler = new QTestMetaStoreHandler(cliConfig.getMetastoreType());
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

  public abstract void beforeClass() throws Exception;

  // HIVE-14444 pending rename: before
  public abstract void setUp() throws Exception;

  // HIVE-14444 pending rename: after
  public abstract void tearDown() throws Exception;

  // HIVE-14444 pending rename: afterClass
  public abstract void shutdown() throws Exception;

  public abstract void runTest(String name, String fileName, String absolutePath) throws Exception;

  public final TestRule buildClassRule() {
    return new TestRule() {
      @Override
      public Statement apply(final Statement base, Description description) {
        return new Statement() {
          @Override
          public void evaluate() throws Throwable {
            metaStoreHandler.setSystemProperties(); // for QTestUtil pre-initialization
            CliAdapter.this.beforeClass(); // instantiating QTestUtil

            LOG.debug("will initialize metastore database in class rule");
            metaStoreHandler.getRule().before();
            metaStoreHandler.getRule().install();

            if (getQt() != null) {
              metaStoreHandler.setMetaStoreConfiguration(getQt().getConf());
              getQt().postInit();
              getQt().newSession();
              getQt().createSources();
            }

            CliAdapter.this.beforeClassSpec();
            try {
              base.evaluate();
            } finally {
              CliAdapter.this.shutdown();
              if (getQt() != null && firstTestNotYetRun) {
                LOG.debug("will destroy metastore database in class rule (if not derby)");
                metaStoreHandler.afterTest(getQt());
              }
            }
          }
        };
      }
    };
  }

  // override this if e.g. a metastore dependent init logic is needed
  protected void beforeClassSpec() throws Exception{
  }

  public final TestRule buildTestRule() {
    return new TestRule() {
      @Override
      public Statement apply(final Statement base, Description description) {
        return new Statement() {
          @Override
          public void evaluate() throws Throwable {

            if (getQt() != null && !firstTestNotYetRun) {
              LOG.debug("will initialize metastore database in test rule");
              metaStoreHandler.setMetaStoreConfiguration(getQt().getConf());
              metaStoreHandler.beforeTest();
            }
            firstTestNotYetRun = false;

            if (getQt() != null && CliAdapter.this.shouldRunCreateScriptBeforeEveryTest()){
              // it's because some drivers still use init scripts, which can create a non-dataset table
              // and get cleant after every test
              getQt().createSources();
            }
            CliAdapter.this.setUp();
            try {
              base.evaluate();
            } finally {
              CliAdapter.this.tearDown();
              if (getQt() != null) {
                LOG.debug("will destroy metastore database in test rule (if not derby)");
                metaStoreHandler.afterTest(getQt());
              }
            }
          }
        };
      }
    };
  }

  protected boolean shouldRunCreateScriptBeforeEveryTest() {
    return false;
  }

  protected abstract QTestUtil getQt();

  // HIVE-14444: pending refactor to push File forward
  public final void runTest(String name, File qfile) throws Exception {
    runTest(name, qfile.getName(), qfile.getAbsolutePath());
  }

}
