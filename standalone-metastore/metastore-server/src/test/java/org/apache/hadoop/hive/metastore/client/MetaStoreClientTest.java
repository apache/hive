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
package org.apache.hadoop.hive.metastore.client;

import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Tests abstract class for running MetaStoreClient API tests.
 */
@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public abstract class MetaStoreClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestDatabases.class);

  // Needed until there is no junit release with @BeforeParam, @AfterParam (junit 4.13)
  // https://github.com/junit-team/junit4/commit/1bf8438b65858565dbb64736bfe13aae9cfc1b5a
  // Then we should remove our own copy
  private static List<AbstractMetaStoreService> metaStoreServices = null;

  @Rule
  public TestRule ignoreRule;

  @Parameterized.Parameters(name = "{0}")
  public static List<Object[]> getMetaStoreToTest() throws Exception {
    List<Object[]> result = MetaStoreFactoryForTests.getMetaStores();
    metaStoreServices = result.stream()
        .map(test -> (AbstractMetaStoreService)test[1])
        .collect(Collectors.toList());
    return result;
  }

  @BeforeClass
  public static void startMetaStores() {
    startMetaStores(new HashMap<MetastoreConf.ConfVars, String>(), new HashMap<String, String>());
  }

  /**
   * Utility method, which can be used to start MetaStore instances with specific configurations
   * different from the default.
   * @param msConf Specific MetaStore configuration values
   * @param extraConf Specific other configuration values
   */
  public static void startMetaStores(Map<MetastoreConf.ConfVars, String> msConf,
      Map<String, String> extraConf) {
    for(AbstractMetaStoreService metaStoreService : metaStoreServices) {
      try {
        metaStoreService.start(msConf, extraConf);
      } catch(Exception e) {
        throw new RuntimeException("Error starting MetaStoreService", e);
      }
    }
  }

  @AfterClass
  public static void stopMetaStores() {
    for(AbstractMetaStoreService metaStoreService : metaStoreServices) {
      try {
        metaStoreService.stop();
      } catch(Exception e) {
        throw new RuntimeException("Error stopping MetaStoreService", e);
      }
    }
  }
}
