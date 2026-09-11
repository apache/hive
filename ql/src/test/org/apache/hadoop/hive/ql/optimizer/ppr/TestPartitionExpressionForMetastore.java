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
package org.apache.hadoop.hive.ql.optimizer.ppr;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFReflect;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Assert;
import org.junit.Test;

/**
 * Verifies that the metastore-side expression deserialization only accepts benign expression
 * graphs: the expression bytes arrive straight from Thrift clients, so classes outside the
 * allowlist, reflect()/reflect2(), and GenericUDFBridge instances pointing at non-UDF classes
 * must all be rejected before anything stringifies or evaluates the expression.
 */
public class TestPartitionExpressionForMetastore {

  @Test
  public void testComparisonExpressionIsAccepted() throws Exception {
    ExprNodeGenericFuncDesc expr = buildExpression(new GenericUDFOPEqual(),
        new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "2026-08-11"));
    String filter = new PartitionExpressionForMetastore().convertExprToFilter(
        SerializationUtilities.serializeObjectWithTypeInformation(expr), null, false);
    Assert.assertNotNull(filter);
  }

  @Test(expected = MetaException.class)
  public void testReflectUdfIsRejected() throws Exception {
    ExprNodeGenericFuncDesc expr = buildExpression(new GenericUDFReflect(),
        new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "java.lang.ProcessBuilder"));
    new PartitionExpressionForMetastore().convertExprToFilter(
        SerializationUtilities.serializeObjectWithTypeInformation(expr), null, false);
  }

  @Test(expected = MetaException.class)
  public void testBridgeToNonUdfClassIsRejected() throws Exception {
    GenericUDFBridge bridge = new GenericUDFBridge("evil", false, "java.lang.ProcessBuilder");
    ExprNodeGenericFuncDesc expr = buildExpression(bridge,
        new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "x"));
    new PartitionExpressionForMetastore().convertExprToFilter(
        SerializationUtilities.serializeObjectWithTypeInformation(expr), null, false);
  }

  @Test(expected = MetaException.class)
  public void testSmuggledClassIsRejected() throws Exception {
    ExprNodeGenericFuncDesc expr = buildExpression(new GenericUDFOPEqual(),
        new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, new java.io.File("/tmp/x")));
    new PartitionExpressionForMetastore().convertExprToFilter(
        SerializationUtilities.serializeObjectWithTypeInformation(expr), null, false);
  }

  private ExprNodeGenericFuncDesc buildExpression(GenericUDF udf, ExprNodeConstantDesc constant) {
    List<ExprNodeDesc> children = new ArrayList<>();
    children.add(new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "ds", "tab", true));
    children.add(constant);
    return new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo, udf, children);
  }
}
