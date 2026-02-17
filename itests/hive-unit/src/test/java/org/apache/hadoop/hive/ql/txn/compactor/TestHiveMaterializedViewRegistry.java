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
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.hadoop.hive.common.MaterializationSnapshot;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.ddl.view.create.CreateMaterializedViewDesc;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveMaterializedViewsRegistry;
import org.apache.hadoop.hive.ql.metadata.HiveRelOptMaterialization;
import org.apache.hadoop.hive.ql.metadata.MaterializedViewMetadata;
import org.apache.hadoop.hive.ql.metadata.RewriteAlgorithm;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveMaterializedViewUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.apache.hadoop.hive.ql.txn.compactor.TestCompactor.executeStatementOnDriver;
import static org.apache.hadoop.hive.ql.txn.compactor.TestCompactor.executeStatementOnDriverSilently;

public class TestHiveMaterializedViewRegistry extends CompactorOnTezTest {

  private static final String DB = "default";
  private static final String TABLE1 = "t1";
  private static final String MV1 = "mat1";

  @Override
  public void setup() throws Exception {
    super.setup();

    executeStatementOnDriverSilently("drop materialized view if exists " + MV1, driver);
    executeStatementOnDriverSilently("drop table if exists" + TABLE1 , driver);

    executeStatementOnDriver("create table " + TABLE1 + "(a int, b string, c float) stored as orc TBLPROPERTIES ('transactional'='true')", driver);
    executeStatementOnDriver("insert into " + TABLE1 + "(a,b, c) values (1, 'one', 1.1), (2, 'two', 2.2), (NULL, NULL, NULL)", driver);
  }

  @Override
  public void tearDown() {
    executeStatementOnDriverSilently("drop materialized view " + MV1, driver);
    executeStatementOnDriverSilently("drop table " + TABLE1 , driver);

    super.tearDown();
  }

  @Test
  public void testRefreshAddsNewMV() throws Exception {
    CreateMaterializedViewDesc createMaterializedViewDesc = createMaterializedViewDesc();
    Table mvTable = createMaterializedViewDesc.toTable(conf);
    Hive.get().createTable(mvTable);

    HiveMaterializedViewsRegistry.get().refresh(Hive.get());

    HiveRelOptMaterialization materialization =
            HiveMaterializedViewsRegistry.get().getRewritingMaterializedView(DB, MV1, RewriteAlgorithm.ALL);

    Assert.assertEquals(DB, materialization.qualifiedTableName.get(0));
    Assert.assertEquals(MV1, materialization.qualifiedTableName.get(1));
  }

  @Test
  public void testRefreshDoesNotAddMVDisabledForRewrite() throws Exception {
    CreateMaterializedViewDesc createMaterializedViewDesc = createMaterializedViewDesc();
    createMaterializedViewDesc.setRewriteEnabled(false);
    Table mvTable = createMaterializedViewDesc.toTable(conf);
    Hive.get().createTable(mvTable);

    HiveMaterializedViewsRegistry.get().refresh(Hive.get());

    HiveRelOptMaterialization materialization =
            HiveMaterializedViewsRegistry.get().getRewritingMaterializedView(DB, MV1, RewriteAlgorithm.ALL);
    Assert.assertNull(materialization);
  }

  @Test
  public void testRefreshUpdatesExistingMV() throws Exception {
    // init the registry
    HiveMaterializedViewsRegistry.get().refresh(Hive.get());

    executeStatementOnDriver("create materialized view " + MV1 + " as " +
            "select a,b,c from " + TABLE1 + " where a > 0 or a is null", driver);

    // replace the MV
    Hive.get().dropTable(DB, MV1);

    CreateMaterializedViewDesc createMaterializedViewDesc = createMaterializedViewDesc();
    Table mvTable = createMaterializedViewDesc.toTable(conf);
    mvTable.setMaterializedViewMetadata(new MaterializedViewMetadata(
            "hive", DB, MV1, new HashSet<>(), new MaterializationSnapshot("anything")));
    Hive.get().createTable(mvTable);

    // test refreshing the registry
    HiveMaterializedViewsRegistry.get().refresh(Hive.get());

    HiveRelOptMaterialization materialization =
            HiveMaterializedViewsRegistry.get().getRewritingMaterializedView(DB, MV1, RewriteAlgorithm.ALL);

    Assert.assertEquals(DB, materialization.qualifiedTableName.get(0));
    Assert.assertEquals(MV1, materialization.qualifiedTableName.get(1));
    Table existingMVTable = HiveMaterializedViewUtils.extractTable(materialization);
    Assert.assertEquals(mvTable.getTTable().getCreateTime(), existingMVTable.getCreateTime());
  }

  @Test
  public void testRefreshRemovesMVDisabledForRewrite() throws Exception {
    // init the registry
    HiveMaterializedViewsRegistry.get().refresh(Hive.get());

    executeStatementOnDriver("create materialized view " + MV1 + " as " +
            "select a,b,c from " + TABLE1 + " where a > 0 or a is null", driver);

    Table mvTable = Hive.get().getTable(DB, MV1);
    mvTable.setRewriteEnabled(false);

    EnvironmentContext environmentContext = new EnvironmentContext();
    environmentContext.putToProperties(StatsSetupConst.DO_NOT_UPDATE_STATS, StatsSetupConst.TRUE);
    Hive.get().alterTable(mvTable, false, environmentContext, true);

    HiveMaterializedViewsRegistry.get().refresh(Hive.get());

    HiveRelOptMaterialization materialization =
            HiveMaterializedViewsRegistry.get().getRewritingMaterializedView(DB, MV1, RewriteAlgorithm.ALL);
    Assert.assertNull(materialization);
  }

  @Test
  public void testRefreshAddsMVEnabledForRewrite() throws Exception {
    // init the registry
    HiveMaterializedViewsRegistry.get().refresh(Hive.get());

    executeStatementOnDriver("create materialized view " + MV1 + " disabled rewrite as " +
            "select a,b,c from " + TABLE1 + " where a > 0 or a is null", driver);

    Table mvTable = Hive.get().getTable(DB, MV1);
    mvTable.setRewriteEnabled(true);

    EnvironmentContext environmentContext = new EnvironmentContext();
    environmentContext.putToProperties(StatsSetupConst.DO_NOT_UPDATE_STATS, StatsSetupConst.TRUE);
    Hive.get().alterTable(mvTable, false, environmentContext, true);

    HiveMaterializedViewsRegistry.get().refresh(Hive.get());

    HiveRelOptMaterialization materialization =
            HiveMaterializedViewsRegistry.get().getRewritingMaterializedView(DB, MV1, RewriteAlgorithm.ALL);

    Assert.assertEquals(DB, materialization.qualifiedTableName.get(0));
    Assert.assertEquals(MV1, materialization.qualifiedTableName.get(1));
  }

  @Test
  public void testRefreshRemovesMVDoesNotExists() throws Exception {
    // init the registry
    HiveMaterializedViewsRegistry.get().refresh(Hive.get());

    executeStatementOnDriver("create materialized view " + MV1 + " as " +
            "select a,b,c from " + TABLE1 + " where a > 0 or a is null", driver);

    Hive.get().dropTable(DB, MV1);

    HiveMaterializedViewsRegistry.get().refresh(Hive.get());

    HiveRelOptMaterialization materialization =
            HiveMaterializedViewsRegistry.get().getRewritingMaterializedView(DB, MV1, RewriteAlgorithm.ALL);
    Assert.assertNull(materialization);
  }

  private static CreateMaterializedViewDesc createMaterializedViewDesc() {
    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put("created_with_ctas", "true");

    CreateMaterializedViewDesc createMaterializedViewDesc = new CreateMaterializedViewDesc(
            MV1,
            null,
            null,
            tableProperties,
            null,
            null,
            null,
            false,
            true,
            "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
            "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
            null,
            "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
            null,
            new HashMap<>());

    createMaterializedViewDesc.setViewOriginalText("select a,b,c from " + TABLE1 + " where a > 0 or a is null");
    createMaterializedViewDesc.setViewExpandedText("select `t1`.`a`,`t1`.`b`,`t1`.`c` from `" + DB + "`.`" + TABLE1 + "` where `t1`.`a` > 0 or `t1`.`a` is null");
    createMaterializedViewDesc.setCols(
            asList(new FieldSchema("a", "int", null),
                    new FieldSchema("b", "string", null),
                    new FieldSchema("c", "float", null)));

    return createMaterializedViewDesc;
  }
}
