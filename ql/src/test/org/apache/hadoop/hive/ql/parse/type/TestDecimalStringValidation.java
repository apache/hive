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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

/**
 * Test strict type checks for comparison operations between decimal and strings. 
 *
 * {@link org.apache.hadoop.hive.conf.HiveConf.ConfVars#HIVE_STRICT_CHECKS_TYPE_SAFETY}
 */
@RunWith(Parameterized.class)
public class TestDecimalStringValidation {

  private static class FunctionCall {
    private final ExprNodeDesc expL;
    private final ExprNodeDesc expR;
    private final FunctionInfo function;

    public FunctionCall(ExprNodeDesc expL, ExprNodeDesc expR, FunctionInfo function) {
      this.expL = expL;
      this.expR = expR;
      this.function = function;
    }

    @Override
    public String toString() {
      return function.getDisplayName() + "(" + expL + "," + expR + ")";
    }
  }

  private final FunctionCall call;

  public TestDecimalStringValidation(FunctionCall call) {
    this.call = call;
  }

  @Parameterized.Parameters(name = "{index}: {0}")
  public static Collection<FunctionCall> params() throws Exception {
    ExprNodeDesc[] characterExps = new ExprNodeDesc[] { 
            new ExprNodeColumnDesc(TypeInfoFactory.varcharTypeInfo, "varchar_col", null, false),
            new ExprNodeColumnDesc(TypeInfoFactory.charTypeInfo, "char_col", null, false),
            new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "string_col", null, false),
            new ExprNodeConstantDesc(TypeInfoFactory.varcharTypeInfo, "123.3"),
            new ExprNodeConstantDesc(TypeInfoFactory.charTypeInfo, "123.3"),
            new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "123.3"), };
    ExprNodeDesc[] numericExps = new ExprNodeDesc[] { 
            new ExprNodeColumnDesc(TypeInfoFactory.decimalTypeInfo, "decimal_col", null, false),
            new ExprNodeConstantDesc(TypeInfoFactory.decimalTypeInfo, 123.3), };
    FunctionInfo[] functions = new FunctionInfo[] { 
            FunctionRegistry.getFunctionInfo("="),
            FunctionRegistry.getFunctionInfo("<"),
            FunctionRegistry.getFunctionInfo(">"),
            FunctionRegistry.getFunctionInfo("<>"),
            FunctionRegistry.getFunctionInfo("<="),
            FunctionRegistry.getFunctionInfo(">="),
            FunctionRegistry.getFunctionInfo("<=>") };
    Collection<FunctionCall> input = new ArrayList<>();
    for (ExprNodeDesc chrExp : characterExps) {
      for (ExprNodeDesc numExp : numericExps) {
        for (FunctionInfo function : functions) {
          input.add(new FunctionCall(chrExp, numExp, function));
          input.add(new FunctionCall(numExp, chrExp, function));
        }
      }
    }
    return input;
  }

  @Test
  public void testValidationDecimalWithCharacterFailsWhenStrictChecksEnabled() {
    HiveConf conf = new HiveConfForTest(getClass());
    conf.setBoolVar(HiveConf.ConfVars.HIVE_STRICT_CHECKS_TYPE_SAFETY, true);
    try {
      validateCall(conf);
      Assert.fail("Validation of " + call + " should fail");
    } catch (Exception e) {
      Assert.assertEquals(HiveConf.StrictChecks.checkTypeSafety(conf), e.getMessage());
    }
  }

  @Test
  public void testValidationDecimalWithCharacterSucceedsWhenStrictChecksDisabled() throws SemanticException {
    HiveConf conf = new HiveConfForTest(getClass());
    conf.setBoolVar(HiveConf.ConfVars.HIVE_STRICT_CHECKS_TYPE_SAFETY, false);
    validateCall(conf);
  }

  private void validateCall(HiveConf conf) throws SemanticException {
    SessionState.start(conf);
    TypeCheckCtx ctx = new TypeCheckCtx(null);
    ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
        .validateUDF(null, false, ctx, call.function, Arrays.asList(call.expL, call.expR));
  }

}
