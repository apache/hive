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
package org.apache.hive;

import org.apache.hadoop.hive.metastore.ServletSecurity;
import org.apache.iceberg.rest.extension.HiveRESTCatalogServerExtension;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.RegisterExtension;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestHiveRESTCatalogClientITOauth2 extends TestHiveRESTCatalogClientITBase {

  @RegisterExtension
  private static final HiveRESTCatalogServerExtension REST_CATALOG_EXTENSION =
      HiveRESTCatalogServerExtension.builder(ServletSecurity.AuthType.OAUTH2)
          .addMetaStoreSchemaClassName(ITestsSchemaInfo.class)
          .build();

  @Override
  public void setupConf() {
    super.setupConf();
    
    // Oauth2 properties
    conf.set(REST_CATALOG_PREFIX + "rest.auth.type", "oauth2");
    conf.set(REST_CATALOG_PREFIX + "oauth2-server-uri", REST_CATALOG_EXTENSION.getOAuth2TokenEndpoint());
    conf.set(REST_CATALOG_PREFIX + "credential", REST_CATALOG_EXTENSION.getOAuth2ClientCredential());
  }

  @Override
  HiveRESTCatalogServerExtension getHiveRESTCatalogServerExtension() {
    return REST_CATALOG_EXTENSION;
  }
}
