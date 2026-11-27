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

import java.util.Objects;
import java.util.Optional;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.BaseMetastoreTableOperations;

/**
 * Hands the Iceberg table a metastore client has just loaded to the storage handler hook that runs right after it,
 * on the same thread, so that a table the client loaded through a catalog is not loaded through it a second time.
 * A client backed by an Iceberg REST catalog builds its metastore tables out of a REST load; without this, the
 * hook would load each of them again.
 *
 * <p>Only the last table of the thread is kept, and only until it is taken: it is handed over only for the
 * metastore table it was offered with, of the same name and metadata location, i.e. the same version.
 */
public final class LoadedTableHandoff {

  private record Loaded(String dbName, String tableName, String metadataLocation, org.apache.iceberg.Table table) {
  }

  private static final ThreadLocal<Loaded> LAST = new ThreadLocal<>();

  private LoadedTableHandoff() {
  }

  /** Offers the Iceberg table the metastore table was just built from. */
  public static void offer(Table hmsTable, org.apache.iceberg.Table icebergTable) {
    String metadataLocation = metadataLocation(hmsTable);
    if (metadataLocation == null) {
      LAST.remove();
      return;
    }
    LAST.set(new Loaded(hmsTable.getDbName(), hmsTable.getTableName(), metadataLocation, icebergTable));
  }

  /**
   * @return the Iceberg table offered for this metastore table, if it was the last one offered on the thread
   */
  public static Optional<org.apache.iceberg.Table> take(Table hmsTable) {
    Loaded loaded = LAST.get();
    if (loaded == null) {
      return Optional.empty();
    }
    LAST.remove();
    String metadataLocation = metadataLocation(hmsTable);
    return metadataLocation != null && metadataLocation.equals(loaded.metadataLocation()) &&
        loaded.dbName().equalsIgnoreCase(hmsTable.getDbName()) &&
        loaded.tableName().equalsIgnoreCase(hmsTable.getTableName()) ?
        Optional.of(loaded.table()) : Optional.empty();
  }

  /** Drops what the thread was offered and has not taken, so that it holds no table once the caller is done. */
  public static void clear() {
    LAST.remove();
  }

  private static String metadataLocation(Table hmsTable) {
    return hmsTable.getParameters() == null ? null :
        Objects.toString(hmsTable.getParameters().get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP), null);
  }
}
