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

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Integration test for Standalone REST Catalog Server with Spring Boot (no auth).
 *
 * Tests that the standalone server can:
 * 1. Start independently of HMS using Spring Boot
 * 2. Connect to an external HMS instance
 * 3. Serve REST Catalog requests
 * 4. Provide health check endpoints (liveness and readiness)
 * 5. Expose Prometheus metrics
 */
@RunWith(SpringRunner.class)
@SpringBootTest(
    classes = BaseStandaloneRESTCatalogServerTest.TestRestCatalogApplication.class,
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
        "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration",
        "spring.main.allow-bean-definition-overriding=true"
    }
)
@TestExecutionListeners(
    listeners = BaseStandaloneRESTCatalogServerTest.HmsStartupListener.class,
    mergeMode = TestExecutionListeners.MergeMode.MERGE_WITH_DEFAULTS
)
public class TestStandaloneRESTCatalogServer extends BaseStandaloneRESTCatalogServerTest {
  @AfterClass
  public static void teardownClass() throws IOException {
    teardownBase();
  }
}
