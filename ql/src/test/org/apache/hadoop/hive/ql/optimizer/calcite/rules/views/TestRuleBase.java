/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.optimizer.calcite.rules.views;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveTypeSystemImpl;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveRelNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.Mock;

import java.util.List;

import static java.util.Arrays.asList;
import static org.mockito.Mockito.doReturn;

public class TestRuleBase {
  protected static final RexBuilder REX_BUILDER = new RexBuilder(new JavaTypeFactoryImpl(new HiveTypeSystemImpl()));
  protected static final RelDataTypeFactory TYPE_FACTORY = REX_BUILDER.getTypeFactory();

  protected static RelOptCluster relOptCluster;
  @Mock
  protected RelOptHiveTable table2Mock;
  protected static RelDataType table2Type;
  protected static Table table2;
  @Mock
  protected static HiveStorageHandler table2storageHandler;

  @BeforeClass
  public static void beforeClass() throws Exception {
    RelOptPlanner planner = CalcitePlanner.createPlanner(new HiveConf());
    relOptCluster = RelOptCluster.create(planner, REX_BUILDER);
    List<RelDataType> t2Schema = asList(
        TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER),
        TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR),
        TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER),
        HiveAugmentSnapshotMaterializationRule.snapshotIdType(TYPE_FACTORY));
    table2Type = TYPE_FACTORY.createStructType(t2Schema, asList("d", "e", "f", VirtualColumn.SNAPSHOT_ID.getName()));
    table2 = new Table();
    table2.setTTable(new org.apache.hadoop.hive.metastore.api.Table());
    table2.setDbName("default");
    table2.setTableName("t2");
  }

  @Before
  public void setup() {
    doReturn(table2Type).when(table2Mock).getRowType();
    doReturn(table2).when(table2Mock).getHiveTableMD();
    table2.setStorageHandler(table2storageHandler);
  }

  protected HiveTableScan createTS() {
    return new HiveTableScan(relOptCluster, relOptCluster.traitSetOf(HiveRelNode.CONVENTION),
        table2Mock, "t2", null, false, false);
  }
}
