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

import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.async.AsyncLoggerContextSelector;
import org.apache.logging.log4j.core.impl.Log4jContextFactory;
import org.apache.logging.log4j.core.selector.ClassLoaderContextSelector;
import org.apache.logging.log4j.core.selector.ContextSelector;

import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * HiveAsyncLogging Test.
 */
public class TestHiveAsyncLogging {

  // this test requires disruptor jar in classpath
  @Test
  public void testAsyncLoggingInitialization() throws Exception {
    HiveConf conf = new HiveConf();
    conf.setBoolVar(ConfVars.HIVE_ASYNC_LOG_ENABLED, false);
    LogUtils.initHiveLog4jCommon(conf, ConfVars.HIVE_LOG4J_FILE);
    Log4jContextFactory log4jContextFactory = (Log4jContextFactory) LogManager.getFactory();
    ContextSelector contextSelector = log4jContextFactory.getSelector();
    assertTrue(contextSelector instanceof ClassLoaderContextSelector);

    conf.setBoolVar(ConfVars.HIVE_ASYNC_LOG_ENABLED, true);
    LogUtils.initHiveLog4jCommon(conf, ConfVars.HIVE_LOG4J_FILE);
    log4jContextFactory = (Log4jContextFactory) LogManager.getFactory();
    contextSelector = log4jContextFactory.getSelector();
    assertTrue(contextSelector instanceof AsyncLoggerContextSelector);
  }
}
