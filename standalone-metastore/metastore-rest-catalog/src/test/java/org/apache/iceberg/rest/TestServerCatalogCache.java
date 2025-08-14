/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.rest;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.ServletSecurity.AuthType;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Files;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.rest.extension.HMSTestCachingCatalog;
import org.apache.iceberg.rest.extension.HMSTestCatalogFactory;
import org.apache.iceberg.rest.extension.HiveRESTCatalogServerExtension;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The test verifies the behavior of the HMSCachingCatalog.
 * <p>
 * The cache relies on the table metadata location to determine whether a table has changed between a cached entry
 * and the latest version in the HMS database.
 * The test will create a table, load it, insert some data, and check that the cache correctly invalidates
 * cached entries when the table is modified.
 * </p>
 */
class TestServerCatalogCache {
  private static final String NEWDB = "newdb";
  private static final String TBL = "tbl";
  private static final String TEMPDIR = "file:/tmp/junit" + Long.toHexString(System.currentTimeMillis()) + "/";
  private static final String ID = "id";
  private static final String DATA = "data";
  private static final Namespace NS = Namespace.of(new String[]{NEWDB});
  private static RESTCatalog restCatalog;
  private static HiveRESTCatalogServerExtension restServer =
          HiveRESTCatalogServerExtension.builder(AuthType.NONE).build(testConfiguration());

  private static Configuration testConfiguration() {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CATALOG_SERVLET_FACTORY, HMSTestCatalogFactory.class.getName());
    return conf;
  }

  private static String baseTableLocation(TableIdentifier identifier) {
    Namespace ns = identifier.namespace();
    return TEMPDIR + ns + "/" + identifier.name();
  }

  @BeforeAll
  static void setupAll() throws Exception {
    restServer.beforeAll(null);
    HMSTestCachingCatalog cachingCatalog = HMSTestCatalogFactory.getLastCatalog();
    Assertions.assertNotNull(cachingCatalog);

    Map<String, String> catalogProperties = new HashMap<>();
    catalogProperties.putIfAbsent("uri", restServer.getRestEndpoint());
    catalogProperties.putIfAbsent("warehouse", "rck_warehouse");
    restCatalog = new RESTCatalog();
    restCatalog.setConf(new Configuration());
    restCatalog.initialize("hive", catalogProperties);
  }

  @BeforeEach
  void setup() {
    try {
      restServer.beforeEach(null);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    RCKUtils.purgeCatalogTestEntries(restCatalog);
  }

  @AfterAll
  static void teardownAll() throws Exception {
    if (restCatalog != null) {
      restCatalog.close();
    }
    restServer.afterAll(null);
    restServer = null;
  }

  @Test
  void testTableCache() {
    restCatalog.createNamespace(NS);
    // acquire the server cache
    HMSTestCachingCatalog cachingCatalog = HMSTestCatalogFactory.getLastCatalog();
    Assertions.assertNotNull(cachingCatalog);
    HiveCatalog catalog = cachingCatalog.getHiveCatalog();
    // create a table schema
    Schema schema = new Schema(
            required(1, ID, Types.IntegerType.get()),
            required(2, DATA, Types.StringType.get()));
    TableIdentifier tableIdent = TableIdentifier.of(NEWDB, TBL);
    String location = baseTableLocation(tableIdent);
    try {
      // create a table in the catalog bypassing the cache
      // using restCatalog would defeat the purpose since it would use the cache
      Table table = catalog
          .buildTable(tableIdent, schema)
          .withLocation(location)
          .create();
      assertThat(table.location()).isEqualTo(location);
      assertThat(table.schema().columns()).hasSize(2);
      // check that the table is *not* yet in the catalog (hit 0, miss 1, load 1)
      Table l0 = cachingCatalog.loadTable(tableIdent);
      assertThat(table.location()).isEqualTo(location);
      assertThat(l0).isNotEqualTo(table);
      Map<String, Integer> m0 = cachingCatalog.getCacheMetrics();
      assertThat(m0).containsEntry("hit", 0);
      assertThat(m0).containsEntry("miss", 1);
      assertThat(m0).containsEntry("invalidation", 0);
      assertThat(m0).containsEntry("load", 1);
      // load the table multiple times, find it in the cache (hit 10)
      for (int i = 0; i < 10; i++) {
        Table l = cachingCatalog.loadTable(tableIdent);
        assertEquals(l0, l);
      }
      // load the table multiple times through the REST catalog, find it in the cache (hit 10)
      for (int i = 0; i < 10; i++) {
        Table l = restCatalog.loadTable(tableIdent);
        // not the same instance, but the same metadata location
        assertEquals(metadataLocation(l0), metadataLocation(l));
      }
      m0 = cachingCatalog.getCacheMetrics();
      assertThat(m0).containsEntry("hit", 20);
      // add rows through table; new snapshot implies invalidation
      insertRows(table);
      // load again, should provoke invalidation (invalidation 1, miss 1, load 2)
      Table l1 = cachingCatalog.loadTable(tableIdent);
      assertThat(l1).isNotEqualTo(l0);
      Map<String, Integer> m2 = cachingCatalog.getCacheMetrics();
      assertThat(m2).containsEntry("invalidation", 1);
      assertThat(m2).containsEntry("miss",1);
      assertThat(m2).containsEntry("load", 2);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      catalog.dropTable(tableIdent);
      HMSTestCatalogFactory.clearLastCatalog();
    }
  }

  private static String metadataLocation(Table table) {
    return table instanceof HasTableOperations tableOps
          ? tableOps.operations().current().metadataFileLocation()
          : null;
  }

  private void insertRows(Table table) throws IOException {
    org.apache.iceberg.data.Record genericRecord = GenericRecord.create(table.schema());
    // write the records to a Parquet file
    final int records = 8;
    URI tempDirUri = URI.create(TEMPDIR);
    File file = Paths.get(tempDirUri).resolve("test_cache.parquet").toFile();
    OutputFile outputFile = Files.localOutput(file);
    try (FileAppender<Record> appender = Parquet.write(outputFile)
            .schema(table.schema())
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .build()) {
      for (int i= 0; i < records; ++i) {
        genericRecord.setField(ID, i);
        genericRecord.setField(DATA, Integer.toBinaryString(i));
        appender.add(genericRecord);
      }
    }
    // create a DataFile from the Parquet file
    DataFile dataFile = DataFiles.builder(table.spec())
            .withPath(file.getAbsolutePath())
            .withFileSizeInBytes(file.length())
            .withRecordCount(records)
            .build();
    // append the DataFile to the table
    table.newAppend()
            .appendFile(dataFile)
            .commit();
  }
}
