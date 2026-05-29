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
public final class IcebergNativeLogicalViewSupport {

  /** Value for HMS {@code table_type} on native Iceberg logical views (uppercase, HMS convention). */
  public static final String ICEBERG_VIEW_HMS_TABLE_TYPE_VALUE =
      HiveOperationsBase.ICEBERG_VIEW_TYPE_VALUE.toUpperCase(java.util.Locale.ENGLISH);

  private IcebergNativeLogicalViewSupport() {
  }

  /**
   * Creates or replaces a view in the Iceberg catalog.
   *
   * @return {@code false} if skipped because the view already exists and {@code replace} is false
   */
  public static boolean createOrReplaceNativeView(
      Configuration conf,
      String databaseName,
      String viewName,
      List<FieldSchema> fieldSchemas,
      String viewSql,
      Map<String, String> tblProperties,
      String comment,
      boolean replace) {

    TableIdentifier identifier = TableIdentifier.of(databaseName, viewName);
    String catalogName = IcebergCatalogProperties.getCatalogName(conf);
    Map<String, String> catalogProps = IcebergCatalogProperties.getCatalogProperties(conf, catalogName);
    Catalog catalog = CatalogUtil.buildIcebergCatalog(catalogName, catalogProps, conf);

    if (catalog instanceof Closeable closeable) {
      try (Closeable ignored = closeable) {
        return commitNativeView(
            catalog, catalogName, identifier, fieldSchemas, viewSql, tblProperties, comment, replace);
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to close Iceberg catalog", e);
      }
    }

    return commitNativeView(
        catalog, catalogName, identifier, fieldSchemas, viewSql, tblProperties, comment, replace);
  }

  private static boolean commitNativeView(
      Catalog catalog,
      String catalogName,
      TableIdentifier identifier,
      List<FieldSchema> fieldSchemas,
      String viewSql,
      Map<String, String> tblProperties,
      String comment,
      boolean replace) {
    ViewCatalog viewCatalog = asViewCatalog(catalog, catalogName);
    if (!replace && viewCatalog.viewExists(identifier)) {
      return false;
    }

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

    for (Map.Entry<String, String> e : tblProps.entrySet()) {
      if (e.getKey() != null && e.getValue() != null) {
        builder = builder.withProperty(e.getKey(), e.getValue());
      }
    }

    if (replace) {
      builder.createOrReplace();
    } else {
      builder.create();
    }
    return true;
  }

  private static ViewCatalog asViewCatalog(Catalog catalog, String catalogName) {
    if (catalog instanceof ViewCatalog viewCatalog) {
      return viewCatalog;
    }
    throw new UnsupportedOperationException(
        String.format(
                "Iceberg catalog '%s' does not implement ViewCatalog.",
                catalogName) +
            " Native views require a catalog that implements ViewCatalog (e.g. HiveCatalog or REST).");
  }
}
