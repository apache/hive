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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveTypeSystemImpl;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveRelNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.TypeConverter;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.mockito.Mockito.doReturn;

public class TestRuleBase {
  protected static final RexBuilder REX_BUILDER = new RexBuilder(new JavaTypeFactoryImpl(new HiveTypeSystemImpl()));
  protected static final RelDataTypeFactory TYPE_FACTORY = REX_BUILDER.getTypeFactory();

  protected static RelOptCluster relOptCluster;

  @Mock
  protected RelOptHiveTable t1NativeMock;
  protected static RelDataType t1NativeType;
  protected static Table t1Native;
  @Mock
  protected static HiveStorageHandler t1NativeStorageHandler;
  @Mock
  protected RelOptHiveTable t2NativeMock;
  protected static RelDataType t2NativeType;
  protected static Table t2Native;
  @Mock
  protected static HiveStorageHandler t2NativeStorageHandler;
  @Mock
  protected RelOptHiveTable t3NativeMock;
  protected static RelDataType t3NativeType;
  protected static Table t3Native;
  @Mock
  protected static HiveStorageHandler t3NativeStorageHandler;

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

    t1Native = createTable("t1");
    t2Native = createTable("t2");
    t3Native = createTable("t3");
    t1NativeType = createTableType(new HashMap<String, SqlTypeName>() {{
      put("a", SqlTypeName.INTEGER);
      put("b", SqlTypeName.VARCHAR);
      put("c", SqlTypeName.INTEGER);
    }}, asList(VirtualColumn.ROWID, VirtualColumn.ROWISDELETED));
    t2NativeType = createTableType(new HashMap<String, SqlTypeName>() {{
      put("d", SqlTypeName.INTEGER);
      put("e", SqlTypeName.VARCHAR);
      put("f", SqlTypeName.INTEGER);
    }}, asList(VirtualColumn.ROWID, VirtualColumn.ROWISDELETED));
    t3NativeType = createTableType(new HashMap<String, SqlTypeName>() {{
      put("g", SqlTypeName.INTEGER);
      put("h", SqlTypeName.VARCHAR);
      put("i", SqlTypeName.INTEGER);
    }}, asList(VirtualColumn.ROWID, VirtualColumn.ROWISDELETED));

    table2 = createTable("t2_iceberg");
    table2Type = createTableType(new HashMap<String, SqlTypeName>() {{
      put("d", SqlTypeName.INTEGER);
      put("e", SqlTypeName.VARCHAR);
      put("f", SqlTypeName.INTEGER);
    }}, singletonList(VirtualColumn.SNAPSHOT_ID));
  }

  private static Table createTable(String name) {
    Table table = new Table();
    table.setTTable(new org.apache.hadoop.hive.metastore.api.Table());
    table.setDbName("default");
    table.setTableName(name);
    return table;
  }

  private static RelDataType createTableType(Map<String, SqlTypeName> columns, Collection<VirtualColumn> virtualColumns)
      throws CalciteSemanticException {
    List<RelDataType> schema = new ArrayList<>(columns.size() + virtualColumns.size());
    List<String> columnNames = new ArrayList<>(columns.size() + virtualColumns.size());
    for (Map.Entry<String, SqlTypeName> column : columns.entrySet()) {
      columnNames.add(column.getKey());
      schema.add(TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(column.getValue()), true));
    }
    for (VirtualColumn virtualColumn : virtualColumns) {
      columnNames.add(virtualColumn.getName());
      schema.add(TypeConverter.convert(virtualColumn.getTypeInfo(), TYPE_FACTORY));
    }
    return TYPE_FACTORY.createStructType(schema, columnNames);
  }

  @Before
  public void setup() {
    doReturn(t1NativeType).when(t1NativeMock).getRowType();
    doReturn(t1Native).when(t1NativeMock).getHiveTableMD();
    t1Native.setStorageHandler(t1NativeStorageHandler);

    doReturn(t2NativeType).when(t2NativeMock).getRowType();
    doReturn(t2Native).when(t2NativeMock).getHiveTableMD();
    t2Native.setStorageHandler(t2NativeStorageHandler);

    doReturn(t3NativeType).when(t3NativeMock).getRowType();
    doReturn(t3Native).when(t3NativeMock).getHiveTableMD();
    t3Native.setStorageHandler(t3NativeStorageHandler);

    doReturn(table2Type).when(table2Mock).getRowType();
    doReturn(table2).when(table2Mock).getHiveTableMD();
    table2.setStorageHandler(table2storageHandler);
  }

  protected RelNode createT2IcebergTS() {
    return createTS(table2Mock, "t2");
  }

  protected HiveTableScan createTS(RelOptHiveTable table, String alias) {
    return new HiveTableScan(relOptCluster, relOptCluster.traitSetOf(HiveRelNode.CONVENTION),
        table, alias, null, false, false);
  }
}
