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

package org.apache.iceberg.mr;

import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * {@link Catalog} loaded from job configuration and closed after use when the implementation
 * supports {@link AutoCloseable}.
 *
 * <p>Used for short-lived catalog access (e.g. REST server-side scan planning in split generation)
 * so HTTP clients and OAuth refresh executors are not leaked on long-lived Tez AMs.
 */
public final class CloseableRestCatalog implements AutoCloseable {

  private final Catalog catalog;

  public static CloseableRestCatalog load(Configuration conf, String catalogName) {
    return Catalogs.loadCatalog(conf, catalogName)
        .map(CloseableRestCatalog::new)
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Iceberg catalog is required for REST server-side scan planning"));
  }

  private CloseableRestCatalog(Catalog catalog) {
    this.catalog = catalog;
  }

  public Table loadTable(String tableIdentifier) {
    return catalog.loadTable(TableIdentifier.parse(tableIdentifier));
  }

  @Override
  public void close() {
    if (catalog instanceof AutoCloseable closeable) {
      try {
        closeable.close();
      } catch (Exception e) {
        throw new UncheckedIOException(
            "Failed to close Iceberg catalog",
            e instanceof IOException ? (IOException) e : new IOException(e));
      }
    }
  }
}
