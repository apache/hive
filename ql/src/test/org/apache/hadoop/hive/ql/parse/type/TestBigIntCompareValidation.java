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

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test that a bigint comparing with a string, varchar or char is not allowed by default.
 *
 */
public class TestBigIntCompareValidation {

  private ExprNodeConstantDesc constant;
  private TypeCheckProcFactory.DefaultExprProcessor processor;
  private String errorMsg;
  private FunctionInfo functionInfo;

  @Before
  public void setUp() throws Exception {
    this.constant = new ExprNodeConstantDesc(TypeInfoFactory.longTypeInfo, 0L);
    this.processor = ExprNodeTypeCheck.getExprNodeDefaultExprProcessor();
    this.errorMsg = HiveConf.StrictChecks.checkTypeSafety(new HiveConf());
    this.functionInfo = FunctionRegistry.getFunctionInfo("=");
  }

  @Test
  public void testCompareWithVarchar() {
    ExprNodeDesc nodeDesc = new ExprNodeColumnDesc(TypeInfoFactory.varcharTypeInfo, "_c0", null, false);
    testValidateUDFOnComparingBigInt(nodeDesc);
  }

  @Test
  public void testCompareWithString() {
    ExprNodeDesc nodeDesc = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "_c1", null, false);
    testValidateUDFOnComparingBigInt(nodeDesc);
  }

  @Test
  public void testCompareWithChar() {
    ExprNodeDesc nodeDesc = new ExprNodeColumnDesc(TypeInfoFactory.charTypeInfo, "_c2", null, false);
    testValidateUDFOnComparingBigInt(nodeDesc);
  }

  private void testValidateUDFOnComparingBigInt(ExprNodeDesc nodeDesc) {
    try {
      TypeCheckCtx ctx = new TypeCheckCtx(null);
      processor.validateUDF(null, false, ctx, functionInfo,
          Lists.newArrayList(constant, nodeDesc));
      Assert.fail("Should throw exception as comparing a bigint and a " + nodeDesc.getTypeString());
    } catch (Exception e) {
      Assert.assertEquals(errorMsg, e.getMessage());
    }
  }
}
