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

package org.apache.iceberg.rest;

import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.CATALOG_SERVLET_AUTH_OAUTH2_VALIDATION_METHOD;

import java.util.Map;
import org.apache.hadoop.hive.metastore.ServletSecurity.AuthType;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.iceberg.rest.extension.HiveRESTCatalogServerExtension;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.extension.RegisterExtension;

@Category(MetastoreCheckinTest.class)
class TestRESTViewCatalogOAuth2TokenIntrospection extends BaseRESTViewCatalogTests {
  @RegisterExtension
  private static final HiveRESTCatalogServerExtension REST_CATALOG_EXTENSION =
      HiveRESTCatalogServerExtension.builder(AuthType.OAUTH2)
          .configure(CATALOG_SERVLET_AUTH_OAUTH2_VALIDATION_METHOD.getVarname(), "introspection").build();

  @Override
  protected Map<String, String> getDefaultClientConfiguration() throws Exception {
    return Map.of(
        "uri", REST_CATALOG_EXTENSION.getRestEndpoint(),
        "rest.auth.type", "oauth2",
        "oauth2-server-uri", REST_CATALOG_EXTENSION.getOAuth2TokenEndpoint(),
        "credential", REST_CATALOG_EXTENSION.getOAuth2ClientCredential()
    );
  }
}
