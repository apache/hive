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
package org.apache.hadoop.hive.conf;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

/**
 * This class is mostly used in unit test environment to prevent polluting the
 * original HiveConf, and also to provide a way to set some default values for execution.
 */
public class HiveConfForTest extends HiveConf {
  private static final Logger LOG = LoggerFactory.getLogger(HiveConfForTest.class.getName());
  private String testDataDir;
  private Map<String, String> overlay = new HashMap<>();

  public HiveConfForTest(Class<?> cls) {
    super(cls);
    init(cls);
  }

  public HiveConfForTest(Configuration configuration, Class<?> cls) {
    super(configuration, cls);
    init(cls);
  }

  private void init(Class<?> cls) {
    initDataDir(cls);
    LOG.info("Using test data dir (class: {}): {}", cls.getName(), testDataDir);
    // HIVE_USER_INSTALL_DIR is required in DagUtils, let's set one
    setValue(HiveConf.ConfVars.HIVE_USER_INSTALL_DIR.varname, testDataDir);
    // to avoid the overhead of starting a tez session when creating a new SessionState
    // many unit tests don't need actual tez execution, and this can save a lot of time
    setValue(HiveConf.ConfVars.HIVE_CLI_TEZ_INITIALIZE_SESSION.varname, "false");
    // to avoid the overhead of using a real yarn cluster for unit tests.
    setValue("tez.local.mode", "true");
    // to avoid the overhead of starting an RPC server of the local DAGAppMaster
    setValue("tez.local.mode.without.network", "true");
    // tests might assume this is set
    setValue("hive.in.tez.test", "true");
    // to prevent polluting the git tree with local tez cache folders (like tez-local-cache136810858646778831)
    setValue("tez.local.cache.root.folder", System.getProperty("build.dir"));
    // prevent RecoveryService from starting, which is not needed in unit tests
    setValue("tez.dag.recovery.enabled", "false");
  }

  public void setValue(String name, String value) {
    overlay.put(name, value);
    super.set(name, value);
  }

  /**
   * Get a unique directory for storing test data, which is based on the class name of the caller unit test
   * and build.dir environment variable (which is set by the surefire plugin, look for root pom.xml).
   * @param cls
   * @return
   */
  private void initDataDir(Class<?> cls) {
    testDataDir = new File(
        String.format("%s/%s-%d", System.getProperty("build.dir"), cls.getCanonicalName(), System.currentTimeMillis()))
            .getPath();
  }

  public String getTestDataDir() {
    return testDataDir;
  }

  /**
   * Get all overlay options as a query string.
   * This can be used in jdbc connection related tests where we want to pass
   * only the specific values which were set in this class.
   */
  public String getOverlayOptionsAsQueryString() {
    return Joiner.on(";").withKeyValueSeparator("=").join(overlay);
  }
}
