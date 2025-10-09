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

package org.apache.hadoop.hive.ql.ddl.table.partition.show;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.type.ExprNodeTypeCheck;
import org.apache.hadoop.hive.ql.parse.type.TypeCheckCtx;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestShowPartitionAnalyzer {

  private HiveConf conf;

  @Before
  public void before() throws Exception {
    conf = new HiveConfForTest(getClass());
    SessionState.start(conf);
  }

  @After
  public void after() throws Exception {
    SessionState.get().close();
  }

  @Test
  public void testGetShowPartitionsFilter() throws Exception {

    List<FieldSchema> partColumns = new ArrayList<FieldSchema>();
    partColumns.add(new FieldSchema("ds", TypeInfoFactory.dateTypeInfo.getTypeName(), null));
    partColumns.add(new FieldSchema("hr", TypeInfoFactory.intTypeInfo.getTypeName(), null));
    partColumns.add(new FieldSchema("rs", TypeInfoFactory.stringTypeInfo.getTypeName(), null));
    RowResolver rwsch = new RowResolver();
    rwsch.put("tableBar", "ds", new ColumnInfo("ds",
        TypeInfoFactory.dateTypeInfo, null, true));
    rwsch.put("tableBar", "hr", new ColumnInfo("hr",
        TypeInfoFactory.intTypeInfo, null, true));
    rwsch.put("tableBar", "rs", new ColumnInfo("rs",
        TypeInfoFactory.stringTypeInfo, null, true));
    TypeCheckCtx tcCtx = new TypeCheckCtx(rwsch);
    // Numeric columns compare with the default partition
    String showPart1 = "show partitions databaseFoo.tableBar " +
        "where ds > '2010-03-03' and "
        + "rs <= 421021";
    ASTNode command = ParseUtils.parse(showPart1, new Context(conf));
    ExprNodeGenericFuncDesc funcDesc = (ExprNodeGenericFuncDesc)genExprNodeByDefault(tcCtx, command);

    // rs <= 421021
    ExprNodeGenericFuncDesc child = (ExprNodeGenericFuncDesc)funcDesc.getChildren().get(1);
    Assert.assertEquals("rs", ((ExprNodeColumnDesc)child.getChildren().get(0)).getColumn());
    Assert.assertEquals(421021, ((ExprNodeConstantDesc)child.getChildren().get(1)).getValue());

    Table table = new Table(new org.apache.hadoop.hive.metastore.api.Table("databaseFoo", "tableBar",
        "foo", 1, 1, -1, null, partColumns, null,
        null, null, TableType.MANAGED_TABLE.name()));
    ShowPartitionAnalyzer analyzer = new ShowPartitionAnalyzer(QueryState.getNewQueryState(
        new HiveConf(), null));
    funcDesc = (ExprNodeGenericFuncDesc)analyzer.getShowPartitionsFilter(table, command);
    Assert.assertTrue(funcDesc.getChildren().size() == 2);
    // ds > '2010-03-03'
    child = (ExprNodeGenericFuncDesc)funcDesc.getChildren().get(0);
    Assert.assertEquals("ds", ((ExprNodeColumnDesc)child.getChildren().get(0)).getColumn());
    Assert.assertEquals(TypeInfoFactory.dateTypeInfo, child.getChildren().get(0).getTypeInfo());
    Assert.assertEquals(child.getChildren().get(0).getTypeString(),
        child.getChildren().get(1).getTypeString());
    // rs <= 421021
    child = (ExprNodeGenericFuncDesc)funcDesc.getChildren().get(1);
    Assert.assertEquals("rs", ((ExprNodeColumnDesc)child.getChildren().get(0)).getColumn());
    Assert.assertEquals(TypeInfoFactory.stringTypeInfo, child.getChildren().get(0).getTypeInfo());
    Assert.assertEquals(child.getChildren().get(0).getTypeString(),
        child.getChildren().get(1).getTypeString());

    // invalid input
    String showPart2 = "show partitions databaseFoo.tableBar " +
        "where hr > 'a123' and hr <= '2346b'";
    command = ParseUtils.parse(showPart2, new Context(conf));
    try {
      analyzer.getShowPartitionsFilter(table, command);
      Assert.fail("show throw semantic exception");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Cannot convert to int from string"));
    }

    funcDesc = (ExprNodeGenericFuncDesc)genExprNodeByDefault(tcCtx, command);
    List<String> partColumnNames = new ArrayList<>();
    List<PrimitiveTypeInfo> partColumnTypeInfos = new ArrayList<>();
    for (FieldSchema fs : partColumns) {
      partColumnNames.add(fs.getName());
      partColumnTypeInfos.add(TypeInfoFactory.getPrimitiveTypeInfo(fs.getType()));
    }

    List<String> partNames = new LinkedList<String>();
    partNames.add("ds=2010-11-10/hr=12/rs=NA");
    partNames.add("ds=2010-11-10/hr=13/rs=AS");
    partNames.add("ds=2010-11-10/hr=23/rs=AE");
    // Metastore use this to filter partition names at default
    PartitionPruner.prunePartitionNames(
        partColumnNames, partColumnTypeInfos, funcDesc, "__HIVE_DEFAULT_PARTITION__", partNames);
    // hr > 'a123' and hr <= '2346b' filter nothing
    Assert.assertTrue(partNames.contains("ds=2010-11-10/hr=12/rs=NA"));
    Assert.assertTrue(partNames.contains("ds=2010-11-10/hr=13/rs=AS"));
    Assert.assertTrue(partNames.contains("ds=2010-11-10/hr=23/rs=AE"));
  }

  private ExprNodeDesc genExprNodeByDefault(TypeCheckCtx tcCtx, ASTNode command) throws Exception {
    for (int childIndex = 0; childIndex < command.getChildCount(); childIndex++) {
      ASTNode astChild = (ASTNode) command.getChild(childIndex);
      if (astChild.getType() == HiveParser.TOK_WHERE) {
        ASTNode conds = (ASTNode) astChild.getChild(0);
        Map<ASTNode, ExprNodeDesc> nodeOutputs = ExprNodeTypeCheck.genExprNode(conds, tcCtx);
        return  nodeOutputs.get(conds);
      }
    }
    return null;
  }
}
