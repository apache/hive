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
package org.apache.impala.catalog;

import org.junit.BeforeClass;
/**
 * Abstract test base class which is used for all the Catalog service tests. These tests
 * expect a Catalog service running and the hostname and the port of the service is
 * available via System properties.
 */
public abstract class CatalogMetastoreTestBase {

  protected static String CATALOGD_HOST;
  protected static int CATALOGD_PORT;

  @BeforeClass
  public static void setup() throws Exception {
    CATALOGD_HOST = System
        .getProperty("CATALOGD_SERVICE_HOST", "localhost");
    CATALOGD_PORT = Integer
        .parseInt(System.getProperty("CATALOGD_HMS_SERVICE_PORT", "5899"));
  }
}
