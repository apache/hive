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

package org.apache.iceberg.hive;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStorePreEventListener;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.events.PreAlterTableEvent;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHiveCatalogRenameConcurrency {

  private static final String DB_NAME = "hivedb";
  private static final TableIdentifier FROM = TableIdentifier.of(DB_NAME, "tbl");
  private static final TableIdentifier TO = TableIdentifier.of(DB_NAME, "tbl_renamed");
  private static final Schema SCHEMA = new Schema(required(1, "id", Types.LongType.get()));

  @RegisterExtension
  private static final HiveMetastoreExtension HIVE_METASTORE_EXTENSION =
      HiveMetastoreExtension.builder()
          .withDatabase(DB_NAME)
          .withConfig(ImmutableMap.of(
              MetastoreConf.ConfVars.PRE_EVENT_LISTENERS.getVarname(), BeforeRenameListener.class.getName()))
          .build();

  private static HiveCatalog catalog;

  @BeforeAll
  public static void initCatalog() {
    catalog = (HiveCatalog) CatalogUtil.loadCatalog(
        HiveCatalog.class.getName(),
        CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE,
        ImmutableMap.of(
            CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
            String.valueOf(TimeUnit.SECONDS.toMillis(10))),
        HIVE_METASTORE_EXTENSION.hiveConf());
  }

  @AfterEach
  public void dropTables() {
    BeforeRenameListener.ACTION.set(null);
    catalog.dropTable(FROM, true);
    catalog.dropTable(TO, true);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRenameFailsOnConcurrentCommit(boolean hiveLockEnabled) {
    catalog.buildTable(FROM, SCHEMA)
        .withProperty(TableProperties.HIVE_LOCK_ENABLED, String.valueOf(hiveLockEnabled))
        .create();
    BeforeRenameListener.ACTION.set(() -> appendFile(catalog.loadTable(FROM)));

    assertThatThrownBy(() -> catalog.renameTable(FROM, TO))
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("modified concurrently");
    assertThat(BeforeRenameListener.ACTION.get()).as("Concurrent commit did not run").isNull();

    assertThat(catalog.tableExists(TO)).isFalse();
    Table table = catalog.loadTable(FROM);
    assertThat(table.currentSnapshot()).as("Concurrent commit was lost").isNotNull();
    long snapshotId = table.currentSnapshot().snapshotId();

    catalog.renameTable(FROM, TO);
    assertThat(catalog.tableExists(FROM)).isFalse();
    assertThat(catalog.loadTable(TO).currentSnapshot().snapshotId()).isEqualTo(snapshotId);
  }

  private static void appendFile(Table table) {
    DataFile file = DataFiles.builder(table.spec())
        .withPath(FileFormat.PARQUET.addExtension(UUID.randomUUID().toString()))
        .withRecordCount(1)
        .withFileSizeInBytes(0)
        .build();
    table.newFastAppend().appendFile(file).commit();
  }

  /**
   * Runs the registered action once, on the HMS side of a rename, after the renaming client read the table
   * and before HMS applies the alter.
   */
  public static class BeforeRenameListener extends MetaStorePreEventListener {
    static final AtomicReference<Runnable> ACTION = new AtomicReference<>();

    public BeforeRenameListener(Configuration config) {
      super(config);
    }

    @Override
    public void onEvent(PreEventContext context) throws MetaException {
      if (context.getEventType() != PreEventContext.PreEventType.ALTER_TABLE) {
        return;
      }
      PreAlterTableEvent event = (PreAlterTableEvent) context;
      if (event.getOldTable().getTableName().equalsIgnoreCase(event.getNewTable().getTableName())) {
        return;
      }
      Runnable action = ACTION.getAndSet(null);
      if (action != null) {
        action.run();
      }
    }
  }
}
