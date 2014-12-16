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

import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorGroupByOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFSumLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncAbsLongToLong;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.ql.udf.generic.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Before;
import org.junit.Test;

public class TestVectorizer {

  static VectorizationContext vContext = null;

  @Before
  public void setUp() {
    List<String> columns = new ArrayList<String>();
    columns.add("col0");
    columns.add("col1");
    columns.add("col2");
    columns.add("col3");

    //Generate vectorized expression
    vContext = new VectorizationContext(columns);
  }

  @Description(name = "fake", value = "FAKE")
  static class FakeGenericUDF extends GenericUDF {
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
      return null;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
      return null;
    }

    @Override
    public String getDisplayString(String[] children) {
      return "fake";
    }
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
    Assert.assertTrue(v.validateMapWorkOperator(gbyOp, null, false));
    VectorGroupByOperator vectorOp = (VectorGroupByOperator) v.vectorizeOperator(gbyOp, vContext);
    Assert.assertEquals(VectorUDAFSumLong.class, vectorOp.getAggregators()[0].getClass());
    VectorUDAFSumLong udaf = (VectorUDAFSumLong) vectorOp.getAggregators()[0];
    Assert.assertEquals(FuncAbsLongToLong.class, udaf.getInputExpression().getClass());
  }

  @Test
  public void testValidateNestedExpressions() {
    ExprNodeColumnDesc col1Expr = new ExprNodeColumnDesc(Integer.class, "col1", "table", false);
    ExprNodeConstantDesc constDesc = new ExprNodeConstantDesc(new Integer(10));

    GenericUDFOPGreaterThan udf = new GenericUDFOPGreaterThan();
    ExprNodeGenericFuncDesc greaterExprDesc = new ExprNodeGenericFuncDesc();
    greaterExprDesc.setTypeInfo(TypeInfoFactory.booleanTypeInfo);
    greaterExprDesc.setGenericUDF(udf);
    List<ExprNodeDesc> children1 = new ArrayList<ExprNodeDesc>(2);
    children1.add(col1Expr);
    children1.add(constDesc);
    greaterExprDesc.setChildren(children1);

    FakeGenericUDF udf2 = new FakeGenericUDF();
    ExprNodeGenericFuncDesc nonSupportedExpr = new ExprNodeGenericFuncDesc();
    nonSupportedExpr.setTypeInfo(TypeInfoFactory.booleanTypeInfo);
    nonSupportedExpr.setGenericUDF(udf2);

    GenericUDFOPAnd andUdf = new GenericUDFOPAnd();
    ExprNodeGenericFuncDesc andExprDesc = new ExprNodeGenericFuncDesc();
    andExprDesc.setTypeInfo(TypeInfoFactory.booleanTypeInfo);
    andExprDesc.setGenericUDF(andUdf);
    List<ExprNodeDesc> children3 = new ArrayList<ExprNodeDesc>(2);
    children3.add(greaterExprDesc);
    children3.add(nonSupportedExpr);
    andExprDesc.setChildren(children3);

    Vectorizer v = new Vectorizer();
    Assert.assertFalse(v.validateExprNodeDesc(andExprDesc, VectorExpressionDescriptor.Mode.FILTER));
    Assert.assertFalse(v.validateExprNodeDesc(andExprDesc, VectorExpressionDescriptor.Mode.PROJECTION));
  }
 
  /**
  * prepareAbstractMapJoin prepares a join operator descriptor, used as helper by SMB and Map join tests. 
  */
  private void prepareAbstractMapJoin(AbstractMapJoinOperator<? extends MapJoinDesc> map, MapJoinDesc mjdesc) {
      mjdesc.setPosBigTable(0);
      List<ExprNodeDesc> expr = new ArrayList<ExprNodeDesc>();
      expr.add(new ExprNodeColumnDesc(Integer.class, "col1", "T", false));
      Map<Byte, List<ExprNodeDesc>> keyMap = new HashMap<Byte, List<ExprNodeDesc>>();
      keyMap.put((byte)0, expr);
      mjdesc.setKeys(keyMap);
      mjdesc.setExprs(keyMap);

      //Set filter expression
      GenericUDFOPEqual udf = new GenericUDFOPEqual();
      ExprNodeGenericFuncDesc equalExprDesc = new ExprNodeGenericFuncDesc();
      equalExprDesc.setTypeInfo(TypeInfoFactory.booleanTypeInfo);
      equalExprDesc.setGenericUDF(udf);
      List<ExprNodeDesc> children1 = new ArrayList<ExprNodeDesc>(2);
      children1.add(new ExprNodeColumnDesc(Integer.class, "col2", "T1", false));
      children1.add(new ExprNodeColumnDesc(Integer.class, "col3", "T2", false));
      equalExprDesc.setChildren(children1);
      List<ExprNodeDesc> filterExpr = new ArrayList<ExprNodeDesc>();
      filterExpr.add(equalExprDesc);
      Map<Byte, List<ExprNodeDesc>> filterMap = new HashMap<Byte, List<ExprNodeDesc>>();
      filterMap.put((byte) 0, expr);
      mjdesc.setFilters(filterMap);
 }

  /**
  * testValidateMapJoinOperator validates that the Map join operator can be vectorized.
  */
  @Test
  public void testValidateMapJoinOperator() {
    MapJoinOperator map = new MapJoinOperator();
    MapJoinDesc mjdesc = new MapJoinDesc();
    
    prepareAbstractMapJoin(map, mjdesc);
    map.setConf(mjdesc);
 
    Vectorizer vectorizer = new Vectorizer();
    Assert.assertTrue(vectorizer.validateMapWorkOperator(map, null, false));
  }

  
  /**
  * testValidateSMBJoinOperator validates that the SMB join operator can be vectorized.
  */
  @Test
  public void testValidateSMBJoinOperator() {
      SMBMapJoinOperator map = new SMBMapJoinOperator();
      SMBJoinDesc mjdesc = new SMBJoinDesc();
      
      prepareAbstractMapJoin(map, mjdesc);
      map.setConf(mjdesc);
    
      Vectorizer vectorizer = new Vectorizer();
      Assert.assertTrue(vectorizer.validateMapWorkOperator(map, null, false)); 
  }
}
