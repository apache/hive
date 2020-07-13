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

import org.antlr.runtime.CommonToken;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Check the genericUDF field of FunctionInfo returned on demand.
 *
 */
public class TestExprProcessorGetFuncExpr {

  @Before
  public void setUp() throws Exception {
    HiveConf hiveConf = new HiveConf();
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_ALLOW_UDF_LOAD_ON_DEMAND, true);
    SessionState sessionState = new SessionState(hiveConf, System.getProperty("user.name"));
    SessionState.setCurrentSessionState(sessionState);
    Function function = new Function("myupper", sessionState.getCurrentDatabase(),
        "org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper", sessionState.getUserName(),
        PrincipalType.USER, (int) (System.currentTimeMillis() / 1000), FunctionType.JAVA, null);
    Hive.get().createFunction(function);
  }

  @Test
  public void testLookupFunctionOnDemand() throws Exception {
    TypeCheckProcFactory.DefaultExprProcessor defaultExprProcessor =
        ExprNodeTypeCheck.getExprNodeDefaultExprProcessor();
    ASTNode funcExpr = new ASTNode(new CommonToken(HiveParser.TOK_FUNCTION, "TOK_FUNCTION"));
    funcExpr.addChild(new ASTNode(new CommonToken(HiveParser.Identifier, "myupper")));
    funcExpr.addChild(new ASTNode(new CommonToken(HiveParser.StringLiteral, "test")));
    List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
    children.add(new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "test"));
    // getXpathOrFuncExprNodeDesc cannot access from outside package
    ExprNodeDesc exprNodeDesc = (ExprNodeDesc) defaultExprProcessor.
        getXpathOrFuncExprNodeDesc(funcExpr, true, children, new TypeCheckCtx(null));
    Assert.assertNotNull(exprNodeDesc);
    Assert.assertNotNull(((ExprNodeGenericFuncDesc)exprNodeDesc).getGenericUDF());
  }

  @After
  public void tearDown() {
    Hive.closeCurrent();
    SessionState.detachSession();
  }
}
