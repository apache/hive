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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.view.BaseView;
import org.apache.iceberg.view.View;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.apache.iceberg.CatalogUtil.ICEBERG_CATALOG_TYPE;
import static org.apache.iceberg.CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestNativeIcebergViewSupport {

  private static final String DB = "native_vw_db";
  private static final String VIEW = "native_vw";

  @RegisterExtension
  private static final HiveMetastoreExtension HIVE_METASTORE_EXTENSION =
      HiveMetastoreExtension.builder().withDatabase(DB).build();

  @AfterEach
  public void dropView() {
    HiveCatalog cat = verifyCatalog();
    TableIdentifier id = TableIdentifier.of(DB, VIEW);
    if (cat.viewExists(id)) {
      cat.dropView(id);
    }
  }

  private HiveConf nativeViewConf() {
    HiveConf conf = new HiveConf(HIVE_METASTORE_EXTENSION.hiveConf());
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CATALOG_DEFAULT, "hive");
    conf.set(
        IcebergCatalogProperties.catalogPropertyConfigKey("hive", ICEBERG_CATALOG_TYPE),
        ICEBERG_CATALOG_TYPE_HIVE);
    return conf;
  }

  private HiveCatalog verifyCatalog() {
    return (HiveCatalog)
        CatalogUtil.loadCatalog(
            HiveCatalog.class.getName(),
            ICEBERG_CATALOG_TYPE_HIVE,
            ImmutableMap.of(
                CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
                String.valueOf(TimeUnit.SECONDS.toMillis(10))),
            HIVE_METASTORE_EXTENSION.hiveConf());
  }

  @Test
  public void testCreateCommitsNativeViewWithMarkerProperty() throws Exception {
    HiveConf conf = nativeViewConf();
    List<FieldSchema> cols =
        Arrays.asList(new FieldSchema("id", "int", null), new FieldSchema("name", "string", null));
    String sql = String.format("select id, name from %s.src_tbl", DB);
    Map<String, String> props = Collections.singletonMap("k1", "v1");

    boolean created =
        NativeIcebergViewSupport.createOrReplaceNativeView(
            conf, DB, VIEW, cols, sql, props, "hello-view", false, false);
    assertThat(created).isTrue();

    HiveCatalog cat = verifyCatalog();
    TableIdentifier id = TableIdentifier.of(DB, VIEW);
    assertThat(cat.viewExists(id)).isTrue();
    View view = cat.loadView(id);
    assertThat(
            view.properties().get(NativeIcebergViewSupport.NATIVE_VIEW_STORAGE_HANDLER_CLASS_PARAM))
        .isEqualTo(NativeIcebergViewSupport.NATIVE_ICEBERG_VIEW_HANDLER_FQCN);
    assertThat(view.properties().get("comment")).isEqualTo("hello-view");
    assertThat(view.properties().get("k1")).isEqualTo("v1");
    HiveViewOperations ops = (HiveViewOperations) ((BaseView) view).operations();
    assertThat(ops.current().currentVersion().representations()).isNotEmpty();
  }

  @Test
  public void testIfNotExistsReturnsFalseWhenViewExists() throws Exception {
    HiveConf conf = nativeViewConf();
    List<FieldSchema> cols = Collections.singletonList(new FieldSchema("id", "int", null));

    assertThat(
            NativeIcebergViewSupport.createOrReplaceNativeView(
                conf, DB, VIEW, cols, "select 1 as id", null, null, false, false))
        .isTrue();
    assertThat(
            NativeIcebergViewSupport.createOrReplaceNativeView(
                conf, DB, VIEW, cols, "select 2 as id", null, null, false, true))
        .isFalse();
  }

  @Test
  public void testReplaceUpdatesView() throws Exception {
    HiveConf conf = nativeViewConf();
    List<FieldSchema> cols = Collections.singletonList(new FieldSchema("id", "int", null));

    NativeIcebergViewSupport.createOrReplaceNativeView(
        conf, DB, VIEW, cols, "select 1 as id", null, null, false, false);
    assertThat(
            NativeIcebergViewSupport.createOrReplaceNativeView(
                conf, DB, VIEW, cols, "select 2 as id", null, null, true, false))
        .isTrue();

    assertThat(verifyCatalog().viewExists(TableIdentifier.of(DB, VIEW))).isTrue();
  }
}
