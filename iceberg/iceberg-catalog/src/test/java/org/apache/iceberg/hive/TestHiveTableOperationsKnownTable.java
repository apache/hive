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

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.exceptions.NoSuchIcebergTableException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Loading a table from a metastore table the caller already has, rather than from the metastore.
 */
public class TestHiveTableOperationsKnownTable extends HiveTableTestBase {

  private final AtomicInteger metastoreCalls = new AtomicInteger();

  @Test
  public void testKnownTableSkipsTheMetastore() throws Exception {
    Table hmsTable = hmsTable();
    HiveTableOperations ops = ops();
    ops.useKnownTable(hmsTable);

    assertThat(ops.current().metadataFileLocation())
        .isEqualTo(hmsTable.getParameters().get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP));
    assertThat(metastoreCalls).hasValue(0);

    // only the next refresh: later ones ask the metastore again
    ops.refresh();
    assertThat(metastoreCalls).hasValue(1);
  }

  @Test
  public void testCommitOnAStaleKnownTable() throws Exception {
    Table stale = hmsTable();
    catalog.loadTable(TABLE_IDENTIFIER).updateProperties().set("by.other.engine", "1").commit();

    HiveTableOperations ops = ops();
    ops.useKnownTable(stale);
    BaseTable table = new BaseTable(ops, TABLE_IDENTIFIER.toString());
    assertThat(table.properties()).doesNotContainKey("by.other.engine");

    // the metastore still has the last word: the commit fails on the stale base, refreshes and is retried
    table.updateProperties().set("by.this.query", "1").commit();

    assertThat(catalog.loadTable(TABLE_IDENTIFIER).properties())
        .containsEntry("by.other.engine", "1")
        .containsEntry("by.this.query", "1");
  }

  @Test
  public void testKnownTableMustBeAnIcebergTable() throws Exception {
    Table notIceberg = hmsTable();
    notIceberg.getParameters().remove(BaseMetastoreTableOperations.TABLE_TYPE_PROP);
    HiveTableOperations ops = ops();
    ops.useKnownTable(notIceberg);

    assertThatThrownBy(ops::current).isInstanceOf(NoSuchIcebergTableException.class);
  }

  @Test
  public void testCatalogIgnoresAKnownTableOfAnotherName() throws Exception {
    Table other = hmsTable();
    other.setTableName("other");
    other.getParameters().put(BaseMetastoreTableOperations.METADATA_LOCATION_PROP, "file:/does/not/exist.json");

    org.apache.iceberg.Table table = catalog.loadTable(TABLE_IDENTIFIER, other);

    assertThat(((BaseTable) table).operations().current().metadataFileLocation())
        .isEqualTo(hmsTable().getParameters().get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP));
  }

  private Table hmsTable() throws TException {
    return HIVE_METASTORE_EXTENSION.metastoreClient().getTable(DB_NAME, TABLE_NAME);
  }

  private HiveTableOperations ops() {
    HiveConf conf = HIVE_METASTORE_EXTENSION.hiveConf();
    return new HiveTableOperations(conf, countingPool(catalog.clientPool()), new HadoopFileIO(conf), null,
        catalog.name(), DB_NAME, TABLE_NAME);
  }

  private ClientPool<IMetaStoreClient, TException> countingPool(ClientPool<IMetaStoreClient, TException> pool) {
    return new ClientPool<>() {
      @Override
      public <R> R run(Action<R, IMetaStoreClient, TException> action) throws TException, InterruptedException {
        metastoreCalls.incrementAndGet();
        return pool.run(action);
      }

      @Override
      public <R> R run(Action<R, IMetaStoreClient, TException> action, boolean retry)
          throws TException, InterruptedException {
        metastoreCalls.incrementAndGet();
        return pool.run(action, retry);
      }
    };
  }
}
