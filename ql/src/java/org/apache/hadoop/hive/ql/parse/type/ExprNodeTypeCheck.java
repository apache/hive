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
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;

/**
 * Class with utility methods to use typecheck processor factory
 * functionality.
 */
public class ExprNodeTypeCheck {

  private ExprNodeTypeCheck() {
    // Defeat instantiation
  }

  /**
   * Given an AST expression and a context, it will produce a map from AST nodes
   * to Hive ExprNode.
   */
  public static Map<ASTNode, ExprNodeDesc> genExprNode(ASTNode expr, TypeCheckCtx tcCtx)
      throws SemanticException {
    TypeCheckProcFactory<ExprNodeDesc> factory =
        new TypeCheckProcFactory<>(new ExprNodeDescExprFactory());
    return factory.genExprNode(expr, tcCtx);
  }

  /**
   * Given an AST join expression and a context, it will produce a map from AST nodes
   * to Hive ExprNode.
   */
  public static Map<ASTNode, ExprNodeDesc> genExprNodeJoinCond(ASTNode expr, TypeCheckCtx tcCtx)
      throws SemanticException {
    JoinCondTypeCheckProcFactory<ExprNodeDesc> typeCheckProcFactory =
        new JoinCondTypeCheckProcFactory<>(new ExprNodeDescExprFactory());
    return typeCheckProcFactory.genExprNode(expr, tcCtx);
  }

  /**
   * Returns the default processor to generate Hive ExprNode from AST nodes.
   */
  public static TypeCheckProcFactory<ExprNodeDesc>.DefaultExprProcessor getExprNodeDefaultExprProcessor() {
    TypeCheckProcFactory<ExprNodeDesc> factory =
        new TypeCheckProcFactory<>(new ExprNodeDescExprFactory());
    return factory.getDefaultExprProcessor();
  }

  /**
   * Transforms column information into the corresponding Hive ExprNode.
   */
  public static ExprNodeDesc toExprNode(ColumnInfo columnInfo, RowResolver rowResolver)
      throws SemanticException {
    ExprNodeDescExprFactory factory = new ExprNodeDescExprFactory();
    return factory.toExpr(columnInfo, rowResolver, 0);
  }

}
