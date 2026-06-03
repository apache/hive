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

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.view.ViewBuilder;

/**
 * Commits a native Iceberg view through the configured default Iceberg catalog (HiveCatalog or REST
 * catalog, etc.) when {@code Catalog} also implements {@link ViewCatalog}.
 */
public final class IcebergLogicalViewSupport {

  private IcebergLogicalViewSupport() {
  }

  /**
   * Loads the native Iceberg logical view definition and applies SQL, schema, and Iceberg params to {@code hmsTable}
   */
  public static void enrichHmsTableFromIcebergView(
      org.apache.hadoop.hive.metastore.api.Table hmsTable, Configuration conf) {
    TableIdentifier identifier = TableIdentifier.of(hmsTable.getDbName(), hmsTable.getTableName());
    String catalogName = IcebergCatalogProperties.getCatalogName(conf);
    Map<String, String> catalogProps = IcebergCatalogProperties.getCatalogProperties(conf, catalogName);
    Catalog catalog = CatalogUtil.buildIcebergCatalog(catalogName, catalogProps, conf);

    try {
      if (catalog instanceof Closeable closeable) {
        try (Closeable ignored = closeable) {
          loadAndApplyView(hmsTable, conf, catalog, catalogName, identifier);
        }
      } else {
        loadAndApplyView(hmsTable, conf, catalog, catalogName, identifier);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close Iceberg catalog", e);
    }
  }

  private static void loadAndApplyView(
      org.apache.hadoop.hive.metastore.api.Table hmsTable,
      Configuration conf,
      Catalog catalog,
      String catalogName,
      TableIdentifier identifier) {
    ViewCatalog viewCatalog = asViewCatalog(catalog, catalogName);
    MetastoreUtil.applyIcebergViewToHmsTable(hmsTable, viewCatalog.loadView(identifier), conf);
  }

  /** Creates or replaces a view in the Iceberg catalog. */
  public static void createOrReplaceView(
      Configuration conf,
      String databaseName,
      String viewName,
      List<FieldSchema> fieldSchemas,
      String viewSql,
      Map<String, String> tblProperties,
      String comment) {

    TableIdentifier identifier = TableIdentifier.of(databaseName, viewName);
    String catalogName = IcebergCatalogProperties.getCatalogName(conf);
    Map<String, String> catalogProps = IcebergCatalogProperties.getCatalogProperties(conf, catalogName);
    Catalog catalog = CatalogUtil.buildIcebergCatalog(catalogName, catalogProps, conf);

    if (catalog instanceof Closeable closeable) {
      try (Closeable ignored = closeable) {
        commitView(catalog, catalogName, identifier, fieldSchemas, viewSql, tblProperties, comment);
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to close Iceberg catalog", e);
      }
    } else {
      commitView(catalog, catalogName, identifier, fieldSchemas, viewSql, tblProperties, comment);
    }
  }

  private static void commitView(
      Catalog catalog,
      String catalogName,
      TableIdentifier identifier,
      List<FieldSchema> fieldSchemas,
      String viewSql,
      Map<String, String> tblProperties,
      String comment) {
    ViewCatalog viewCatalog = asViewCatalog(catalog, catalogName);

    ViewBuilder builder =
        viewCatalog
            .buildView(identifier)
            .withSchema(HiveSchemaUtil.convert(fieldSchemas, Collections.emptyMap(), true))
            .withDefaultNamespace(Namespace.of(identifier.namespace().level(0)))
            .withQuery("hive", viewSql);

    if (StringUtils.isNotBlank(comment)) {
      builder = builder.withProperty("comment", comment);
    }

    Map<String, String> tblProps =
        tblProperties == null ? Maps.newHashMap() : Maps.newHashMap(tblProperties);

    builder.withProperties(tblProps);

    builder.createOrReplace();
  }

  private static ViewCatalog asViewCatalog(Catalog catalog, String catalogName) {
    if (catalog instanceof ViewCatalog viewCatalog) {
      return viewCatalog;
    }
    throw new UnsupportedOperationException(
        String.format(
                "Iceberg catalog '%s' does not implement ViewCatalog.",
                catalogName) +
            " Iceberg views require a catalog that implements ViewCatalog (e.g. HiveCatalog or REST).");
  }
}
