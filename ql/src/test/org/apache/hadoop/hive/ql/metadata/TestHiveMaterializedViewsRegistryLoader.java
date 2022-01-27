package org.apache.hadoop.hive.ql.metadata;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveMaterializedViewUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.hadoop.hive.ql.metadata.TestMaterializedViewsCache.createMaterialization;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.when;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
class TestHiveMaterializedViewsRegistryLoader {
  private final HiveConf conf = new HiveConf();
  private final HiveMaterializedViewsRegistry.MaterializedViewObjects materializedViewObjects =
          Mockito.mock(HiveMaterializedViewsRegistry.MaterializedViewObjects.class);

  private final HiveMaterializedViewsRegistry.InMemoryMaterializedViewsRegistry materializedViewsRegistry =
          new HiveMaterializedViewsRegistry.InMemoryMaterializedViewsRegistry(
                  (conf1, table) -> createMaterialization(table));

  private Table MV1;
  private Table MV2;

  @BeforeEach
  void setUp() {
    MV1 = new Table(new org.apache.hadoop.hive.metastore.api.Table());
    MV1.setDbName("default");
    MV1.setTableName("mat1");
    MV1.setRewriteEnabled(true);
    MV1.setViewExpandedText("select a, b from t1");
    MV1.setCreateTime(1000);
    CreationMetadata creationMetadata1 = new CreationMetadata();
    creationMetadata1.setMaterializationTime(1000);
    MV1.setMaterializedViewMetadata(new MaterializedViewMetadata(creationMetadata1));

    MV2 = new Table(new org.apache.hadoop.hive.metastore.api.Table());
    MV2.setDbName("default");
    MV2.setTableName("mat2");
    MV2.setRewriteEnabled(true);
    MV2.setViewExpandedText("select a, b from t2 group by a, b");
    MV2.setCreateTime(1000);
    CreationMetadata creationMetadata2 = new CreationMetadata();
    creationMetadata2.setMaterializationTime(1000);
    MV2.setMaterializedViewMetadata(new MaterializedViewMetadata(creationMetadata2));
  }

  @Test
  void testCacheRemainsEmptyWhenNoMVIsPersisted() throws HiveException {
    when(materializedViewObjects.getAllMaterializedViewObjectsForRewriting()).thenReturn(emptyList());

    HiveMaterializedViewsRegistry.Loader loader =
            new HiveMaterializedViewsRegistry.Loader(conf, materializedViewsRegistry, materializedViewObjects);
    loader.refresh();

    assertThat(materializedViewsRegistry.getRewritingMaterializedViews().isEmpty(), is(true));
  }

  @Test
  void testPersistedMVsAreCachedAtRefresh() throws HiveException {
    List<Table> mvs = Arrays.asList(MV1, MV2);
    when(materializedViewObjects.getAllMaterializedViewObjectsForRewriting()).thenReturn(mvs);

    HiveMaterializedViewsRegistry.Loader loader = new HiveMaterializedViewsRegistry.Loader(conf, materializedViewsRegistry, materializedViewObjects);
    loader.refresh();

    assertThat(materializedViewsRegistry.getRewritingMaterializedViews().size(), is(2));

    List<Table> actualMVs = new ArrayList<>();
    for (HiveRelOptMaterialization relOptMaterialization : materializedViewsRegistry.getRewritingMaterializedViews()) {
      actualMVs.add(HiveMaterializedViewUtils.extractTable(relOptMaterialization));
    }

    assertThat(actualMVs, containsInAnyOrder(mvs.toArray()));
  }

  @Test
  void testDroppedMVsAreRemovedFromCachedAtRefresh() throws HiveException {
    materializedViewsRegistry.createMaterializedView(conf, MV1);
    materializedViewsRegistry.createMaterializedView(conf, MV2);
    when(materializedViewObjects.getAllMaterializedViewObjectsForRewriting()).thenReturn(singletonList(MV1));

    HiveMaterializedViewsRegistry.Loader loader =
            new HiveMaterializedViewsRegistry.Loader(conf, materializedViewsRegistry, materializedViewObjects);
    loader.refresh();

    assertThat(materializedViewsRegistry.getRewritingMaterializedViews().size(), is(1));
    assertThat(HiveMaterializedViewUtils.extractTable(materializedViewsRegistry.getRewritingMaterializedViews().get(0)), is(MV1));
  }

  @Test
  void testMVIsRefreshedOnlyIfANewerExistsCheckedByCreationTime() throws HiveException {
    Table newerMV1 = new Table(new org.apache.hadoop.hive.metastore.api.Table());
    newerMV1.setDbName(MV1.getDbName());
    newerMV1.setTableName(MV1.getTableName());
    newerMV1.setRewriteEnabled(MV1.isRewriteEnabled());
    newerMV1.setViewExpandedText(MV1.getViewExpandedText());
    newerMV1.setCreateTime(1200);
    CreationMetadata creationMetadata1 = new CreationMetadata();
    creationMetadata1.setMaterializationTime(1000);
    newerMV1.setMaterializedViewMetadata(new MaterializedViewMetadata(creationMetadata1));

    testMVIsRefreshedOnlyIfANewerExists(newerMV1);
  }

  @Test
  void testMVIsRefreshedOnlyIfANewerExistsCheckedByMaterializationTime() throws HiveException {
    Table newerMV1 = new Table(new org.apache.hadoop.hive.metastore.api.Table());
    newerMV1.setDbName(MV1.getDbName());
    newerMV1.setTableName(MV1.getTableName());
    newerMV1.setRewriteEnabled(MV1.isRewriteEnabled());
    newerMV1.setViewExpandedText(MV1.getViewExpandedText());
    newerMV1.setCreateTime(MV1.getCreateTime());
    CreationMetadata creationMetadata1 = new CreationMetadata();
    creationMetadata1.setMaterializationTime(1200);
    newerMV1.setMaterializedViewMetadata(new MaterializedViewMetadata(creationMetadata1));

    testMVIsRefreshedOnlyIfANewerExists(newerMV1);
  }

  private void testMVIsRefreshedOnlyIfANewerExists(Table newerMV1) throws HiveException {
    materializedViewsRegistry.createMaterializedView(conf, newerMV1);

    when(materializedViewObjects.getAllMaterializedViewObjectsForRewriting()).thenReturn(singletonList(MV1));

    HiveMaterializedViewsRegistry.Loader loader =
            new HiveMaterializedViewsRegistry.Loader(conf, materializedViewsRegistry, materializedViewObjects);
    loader.refresh();

    assertThat(materializedViewsRegistry.getRewritingMaterializedViews().size(), is(1));
    assertThat(HiveMaterializedViewUtils.extractTable(materializedViewsRegistry.getRewritingMaterializedViews().get(0)), is(newerMV1));
  }
}
