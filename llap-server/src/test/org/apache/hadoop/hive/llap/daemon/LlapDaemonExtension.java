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
package org.apache.hadoop.hive.llap.daemon;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.LlapDaemonInfo;
import org.apache.hadoop.hive.llap.daemon.impl.LlapDaemon;
import org.apache.hadoop.hive.llap.metrics.LlapMetricsSystem;
import org.apache.hadoop.hive.llap.metrics.MetricsUtils;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A JUnit Jupiter extension providing ready to use {@link LlapDaemon} for the duration of a test when the test method
 * has a parameter of the respective type.
 * 
 * A new daemon is created before each test and destroyed afterwards. The daemons rely on various static constructs so
 * we cannot have many in the same JVM. If there are many {@link LlapDaemon} parameters in the same test method the same
 * object is returned for all of them.
 * 
 * The class is thread-safe but there cannot be two tests using LLAP daemons concurrently for the reasons mentioned
 * previously.
 */
public class LlapDaemonExtension implements ParameterResolver, BeforeEachCallback, AfterEachCallback {

  private static final ReentrantLock LOCK = new ReentrantLock();
  private static final String[] METRICS_SOURCES = new String[] { 
          "JvmMetrics", 
          "LlapDaemonExecutorMetrics-" + MetricsUtils.getHostName(),
          "LlapDaemonJvmMetrics-" + MetricsUtils.getHostName(),
          MetricsUtils.METRICS_PROCESS_NAME };
  private static LlapDaemon daemon = null;

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    if (!LOCK.tryLock(1, TimeUnit.MINUTES)) {
      throw new IllegalStateException("Lock acquisition failed cause another test is using the LlapDaemon.");
    }
    final String appName = "testLlapDaemon" + context.getUniqueId();
    HiveConf conf = new HiveConf();
    HiveConf.setVar(conf, HiveConf.ConfVars.LLAP_DAEMON_SERVICE_HOSTS, "llap");
    LlapDaemonInfo.initialize(appName, conf);
    daemon =
        new LlapDaemon(conf, 1, LlapDaemon.getTotalHeapSize(), false, false, -1, new String[1], 0, false, 0, 0, 0, 0,
            appName);
    daemon.init(conf);
    daemon.start();
  }

  @Override
  public void afterEach(ExtensionContext context) {
    try {
      daemon.stop();
      for (String ms : METRICS_SOURCES) {
        LlapMetricsSystem.instance().unregisterSource(ms);
      }
      daemon = null;
    } finally {
      LOCK.unlock();
    }
  }

  @Override
  public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return parameterContext.getParameter().getType() == LlapDaemon.class;
  }

  @Override
  public Object resolveParameter(ParameterContext parameterContext, ExtensionContext context)
      throws ParameterResolutionException {
    assert daemon != null;
    return daemon;
  }

}
