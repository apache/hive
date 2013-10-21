/**
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

package org.apache.hadoop.hive.ql.optimizer.physical;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorGroupByOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFSumLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncAbsLongToLong;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFAbs;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Before;
import org.junit.Test;

public class TestVectorizer {

  static VectorizationContext vContext = null;

  @Before
  public void setUp() {
    Map<String, Integer> columnMap = new HashMap<String, Integer>();
    columnMap.put("col1", 0);
    columnMap.put("col2", 1);

    //Generate vectorized expression
    vContext = new VectorizationContext(columnMap, 2);
  }

  @Test
  public void testAggregateOnUDF() throws HiveException {
    AggregationDesc aggDesc = new AggregationDesc();
    aggDesc.setGenericUDAFName("sum");
    ExprNodeGenericFuncDesc exprNodeDesc = new ExprNodeGenericFuncDesc();
    exprNodeDesc.setTypeInfo(TypeInfoFactory.intTypeInfo);
    ArrayList<ExprNodeDesc> params = new ArrayList<ExprNodeDesc>();
    params.add(exprNodeDesc);
    aggDesc.setParameters(params);
    GenericUDFAbs absUdf = new GenericUDFAbs();
    exprNodeDesc.setGenericUDF(absUdf);
    List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
    ExprNodeColumnDesc colExprA = new ExprNodeColumnDesc(Integer.class, "col1", "T", false);
    ExprNodeColumnDesc colExprB = new ExprNodeColumnDesc(Integer.class, "col2", "T", false);
    children.add(colExprA);
    exprNodeDesc.setChildren(children);

    ArrayList<String> outputColumnNames = new ArrayList<String>();
    outputColumnNames.add("_col0");

    GroupByDesc desc = new GroupByDesc();
    desc.setOutputColumnNames(outputColumnNames);
    ArrayList<AggregationDesc> aggDescList = new ArrayList<AggregationDesc>();
    aggDescList.add(aggDesc);

    desc.setAggregators(aggDescList);

    ArrayList<ExprNodeDesc> grpByKeys = new ArrayList<ExprNodeDesc>();
    grpByKeys.add(colExprB);
    desc.setKeys(grpByKeys);

    GroupByOperator gbyOp = new GroupByOperator();
    gbyOp.setConf(desc);

    Vectorizer v = new Vectorizer();
    Assert.assertTrue(v.validateOperator(gbyOp));
    VectorGroupByOperator vectorOp = (VectorGroupByOperator) v.vectorizeOperator(gbyOp, vContext);
    Assert.assertEquals(VectorUDAFSumLong.class, vectorOp.getAggregators()[0].getClass());
    VectorUDAFSumLong udaf = (VectorUDAFSumLong) vectorOp.getAggregators()[0];
    Assert.assertEquals(FuncAbsLongToLong.class, udaf.getInputExpression().getClass());
  }
}
