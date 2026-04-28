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
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
public final class NativeIcebergViewSupport {

  /**
   * HMS table parameter recording the storage handler FQCN for native views; must match the constant
   * {@code NATIVE_VIEW_STORAGE_HANDLER_CLASS_PARAM} on {@code org.apache.hadoop.hive.ql.metadata.HiveStorageHandler}.
   */
  public static final String NATIVE_VIEW_STORAGE_HANDLER_CLASS_PARAM = "hive.storage.native.view.handler";

  /** Value stored with {@link #NATIVE_VIEW_STORAGE_HANDLER_CLASS_PARAM} for Iceberg native views. */
  public static final String NATIVE_ICEBERG_VIEW_HANDLER_FQCN = "org.apache.iceberg.mr.hive.HiveIcebergStorageHandler";

  /**
   * HMS / Iceberg view marker entries for a native Iceberg catalog view (same map as
   * {@code HiveIcebergStorageHandler#getNativeViewHmsTableProperties()}).
   */
  public static Map<String, String> defaultNativeViewMarkerTableProperties() {
    return Map.of(NATIVE_VIEW_STORAGE_HANDLER_CLASS_PARAM, NATIVE_ICEBERG_VIEW_HANDLER_FQCN);
  }

  private NativeIcebergViewSupport() {
  }

  /**
   * Creates or replaces a view in the Iceberg catalog.
   *
   * @return {@code false} if skipped because {@code ifNotExists} is true and the view already exists
   */
  public static boolean createOrReplaceNativeView(Configuration conf, String databaseName, String viewName,
      List<FieldSchema> fieldSchemas, String viewSql, Map<String, String> tblProperties, String comment,
      boolean replace, boolean ifNotExists) throws Exception {

    TableIdentifier identifier = TableIdentifier.of(databaseName, viewName);
    String catalogName = IcebergCatalogProperties.getCatalogName(conf);
    Map<String, String> catalogProps = IcebergCatalogProperties.getCatalogProperties(conf, catalogName);
    Catalog catalog = CatalogUtil.buildIcebergCatalog(catalogName, catalogProps, conf);
    try {
      ViewCatalog viewCatalog = asViewCatalog(catalog, catalogName);
      if (!replace && ifNotExists && viewCatalog.viewExists(identifier)) {
        return false;
      }

      ViewBuilder builder = startViewBuilder(viewCatalog, identifier, fieldSchemas, viewSql);
      builder = applyCommentAndTblProps(builder, mergeDefaultNativeViewTableProperties(tblProperties), comment);
      commitView(builder, replace);
      return true;
    } finally {
      if (catalog instanceof Closeable) {
        ((Closeable) catalog).close();
      }
    }
  }

  private static ViewCatalog asViewCatalog(Catalog catalog, String catalogName) {
    if (!(catalog instanceof ViewCatalog)) {
      throw new UnsupportedOperationException(
          String.format(
                  "Iceberg catalog '%s' does not implement ViewCatalog.",
                  catalogName) +
              " Native views require a catalog that implements ViewCatalog (e.g. HiveCatalog or REST).");
    }
    return (ViewCatalog) catalog;
  }

  private static ViewBuilder startViewBuilder(
      ViewCatalog viewCatalog,
      TableIdentifier identifier,
      List<FieldSchema> fieldSchemas,
      String viewSql) {
    return viewCatalog
        .buildView(identifier)
        .withSchema(HiveSchemaUtil.convert(fieldSchemas, Collections.emptyMap(), true))
        .withDefaultNamespace(Namespace.of(identifier.namespace().level(0)))
        .withQuery("hive", viewSql);
  }

  /**
   * Fills Iceberg native-view HMS / view marker properties when absent (e.g. direct catalog callers).
   * Handlers that delegate here after {@code HiveStorageHandler#getNativeViewHmsTableProperties()} already
   * supplied markers get the same result.
   */
  public static Map<String, String> mergeDefaultNativeViewTableProperties(Map<String, String> tblProperties) {
    Map<String, String> merged = Maps.newHashMap();
    if (tblProperties != null) {
      merged.putAll(tblProperties);
    }
    for (Map.Entry<String, String> e : defaultNativeViewMarkerTableProperties().entrySet()) {
      merged.putIfAbsent(e.getKey(), e.getValue());
    }
    return merged;
  }

  private static ViewBuilder applyCommentAndTblProps(
      ViewBuilder builder, Map<String, String> tblProperties, String comment) {
    ViewBuilder viewBuilder = builder;
    if (comment != null && !comment.isEmpty()) {
      viewBuilder = viewBuilder.withProperty("comment", comment);
    }
    if (tblProperties != null) {
      for (Map.Entry<String, String> e : tblProperties.entrySet()) {
        if (e.getKey() != null && e.getValue() != null) {
          viewBuilder = viewBuilder.withProperty(e.getKey(), e.getValue());
        }
      }
    }
    return viewBuilder;
  }

  private static void commitView(ViewBuilder builder, boolean replace) {
    if (replace) {
      builder.createOrReplace();
    } else {
      builder.create();
    }
  }
}
