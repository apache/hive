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
package org.apache.hadoop.hive.ql.parse.type;

import java.util.Map;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class RexNodeTypeCheck {

  /**
   * Given an AST expression and a context, it will produce a map from AST nodes
   * to Calcite RexNode.
   */
  public static Map<ASTNode, RexNode> genExprNode(ASTNode expr, TypeCheckCtx tcCtx)
      throws SemanticException {
    TypeCheckProcFactory<RexNode> factory =
        new TypeCheckProcFactory<>(new RexNodeExprFactory(tcCtx.getRexBuilder()));
    return factory.genExprNode(expr, tcCtx);
  }

  /**
   * Returns the default processor to generate Calcite RexNode from AST nodes.
   */
  public static TypeCheckProcFactory<RexNode>.DefaultExprProcessor getExprNodeDefaultExprProcessor(RexBuilder rexBuilder) {
    TypeCheckProcFactory<RexNode> factory =
        new TypeCheckProcFactory<>(new RexNodeExprFactory(rexBuilder));
    return factory.getDefaultExprProcessor();
  }

  /**
   * Given an AST join expression and a context, it will produce a map from AST nodes
   * to Calcite RexNode.
   */
  public static Map<ASTNode, RexNode> genExprNodeJoinCond(ASTNode expr, TypeCheckCtx tcCtx, RexBuilder rexBuilder)
      throws SemanticException {
    JoinCondTypeCheckProcFactory<RexNode> typeCheckProcFactory =
        new JoinCondTypeCheckProcFactory<>(new RexNodeExprFactory(rexBuilder));
    return typeCheckProcFactory.genExprNode(expr, tcCtx);
  }

  /**
   * Transforms column information into the corresponding Calcite RexNode.
   */
  public static RexNode toExprNode(ColumnInfo columnInfo, RowResolver rowResolver, int offset, RexBuilder rexBuilder)
      throws SemanticException {
    RexNodeExprFactory factory = new RexNodeExprFactory(rexBuilder);
    return factory.toExpr(columnInfo, rowResolver, offset);
  }

  public static RexNode genConstraintsExpr(
      HiveConf conf, RexBuilder rexBuilder, Table targetTable, boolean updateStatement, RowResolver inputRR)
      throws SemanticException {
    return new ConstraintExprGenerator<>(conf, new TypeCheckProcFactory<>(new RexNodeExprFactory(rexBuilder)))
        .genConstraintsExpr(targetTable, updateStatement, inputRR);
  }
}
