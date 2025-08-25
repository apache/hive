/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

@RunWith(Parameterized.class)
public class TestIcebergLlapRESTCatalogLocalCliDriver {

  private static final Logger LOG = LoggerFactory.getLogger(
      org.apache.hadoop.hive.cli.TestIcebergLlapRESTCatalogLocalCliDriver.class);
  private static final String CATALOG_NAME = "rest_catalog";
  private static final CliAdapter adapter = new CliConfigs.TestIcebergLlapRESTCatalogLocalCliDriver().getCliAdapter();
  
  private final String name;
  private final File qfile;
  
  @ClassRule
  public static final HiveRESTCatalogServerExtension REST_CATALOG_EXTENSION =
      HiveRESTCatalogServerExtension.builder(ServletSecurity.AuthType.NONE)
          .addMetaStoreSchemaClassName(ITestsSchemaInfo.class)
          .build();

  @ClassRule
  public static final TestRule cliClassRule = adapter.buildClassRule();

  @Rule
  public final TestRule cliTestRule = adapter.buildTestRule();

  @Parameters(name = "{0}")
  public static List<Object[]> getParameters() throws Exception {
    return adapter.getParameters();
  }

  public TestIcebergLlapRESTCatalogLocalCliDriver(String name, File qfile) {
    this.name = name;
    this.qfile = qfile;
  }

  @Before
  public void setupHiveConfig() {
    String restCatalogType = CatalogUtil.ICEBERG_CATALOG_TYPE_REST;
    String restCatalogPrefix = String.format(CatalogUtils.CUSTOM_CATALOG_CONFIG_PREFIX, restCatalogType);

    Configuration conf = SessionState.get().getConf();
    conf.set(CatalogUtils.CATALOG_CONFIG_TYPE, restCatalogType);
    conf.set(MetastoreConf.ConfVars.METASTORE_CLIENT_IMPL.getVarname(),
        "org.apache.iceberg.hive.client.HiveRESTCatalogClient");
    conf.set(restCatalogPrefix + ".uri", REST_CATALOG_EXTENSION.getRestEndpoint());
    conf.set(MetastoreConf.ConfVars.CATALOG_DEFAULT.getVarname(), CATALOG_NAME);
  }

  @Before
  public void cleanUpRestCatalogServerTmpDir() throws IOException {
    try (Stream<Path> children = Files.list(REST_CATALOG_EXTENSION.getRestCatalogServer().getWarehouseDir())) {
      children
          .filter(path -> !path.getFileName().toString().equals("derby.log"))
          .filter(path -> !path.getFileName().toString().equals("metastore_db"))
          .forEach(path -> {
            try {
              if (Files.isDirectory(path)) {
                FileUtils.deleteDirectory(path.toFile());
              } else {
                Files.delete(path);
              }
            } catch (IOException e) {
              LOG.error("Failed to delete path: {}", path, e);
            }
          });
    }
  }

  @Test
  public void testCliDriver() throws Exception {
    adapter.runTest(name, qfile);
  }
}