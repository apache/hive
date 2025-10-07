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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.cli.control.CliAdapter;
import org.apache.hadoop.hive.cli.control.CliConfigs;
import org.apache.hadoop.hive.metastore.ServletSecurity;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.ITestsSchemaInfo;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.hive.CatalogUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.io.IOException;
import java.util.List;

@RunWith(Parameterized.class)
public class TestIcebergRESTCatalogHMSLlapLocalCliDriver {

  private static final String CATALOG_NAME = "ice01";
  private static final CliAdapter CLI_ADAPTER =
      new CliConfigs.TestIcebergRESTCatalogHMSLlapLocalCliDriver().getCliAdapter();
  
  private final String name;
  private final File qfile;
  
  @ClassRule
  public static final HiveRESTCatalogServerExtension REST_CATALOG_EXTENSION =
      HiveRESTCatalogServerExtension.builder(ServletSecurity.AuthType.OAUTH2)
          .addMetaStoreSchemaClassName(ITestsSchemaInfo.class)
          .build();

  @ClassRule
  public static final TestRule CLI_CLASS_RULE = CLI_ADAPTER.buildClassRule();

  @Rule
  public final TestRule cliTestRule = CLI_ADAPTER.buildTestRule();

  @Parameters(name = "{0}")
  public static List<Object[]> getParameters() throws Exception {
    return CLI_ADAPTER.getParameters();
  }

  public TestIcebergRESTCatalogHMSLlapLocalCliDriver(String name, File qfile) {
    this.name = name;
    this.qfile = qfile;
  }

  @Before
  public void setupHiveConfig() {
    String restCatalogPrefix = String.format("%s%s.", CatalogUtils.CATALOG_CONFIG_PREFIX, CATALOG_NAME);

    Configuration conf = SessionState.get().getConf();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_CLIENT_IMPL,
        "org.apache.iceberg.hive.client.HiveRESTCatalogClient");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CATALOG_DEFAULT, CATALOG_NAME);
    conf.set(restCatalogPrefix + "uri", REST_CATALOG_EXTENSION.getRestEndpoint());
    conf.set(restCatalogPrefix + "type", CatalogUtil.ICEBERG_CATALOG_TYPE_REST);

    // auth configs
    conf.set(restCatalogPrefix + "rest.auth.type", "oauth2");
    conf.set(restCatalogPrefix + "oauth2-server-uri", REST_CATALOG_EXTENSION.getOAuth2TokenEndpoint());
    conf.set(restCatalogPrefix + "credential", REST_CATALOG_EXTENSION.getOAuth2ClientCredential());
  }
  
  @After
  public void tearDown() throws IOException {
    FileUtils.deleteDirectory(REST_CATALOG_EXTENSION.getRestCatalogServer().getWarehouseDir().toFile());
  }

  @Test
  public void testCliDriver() throws Exception {
    CLI_ADAPTER.runTest(name, qfile);
  }
}
