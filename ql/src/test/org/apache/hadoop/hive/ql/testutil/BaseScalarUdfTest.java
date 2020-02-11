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

package org.apache.hadoop.hive.ql.testutil;

import java.util.List;



import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.CollectOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.CollectDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Ignore;

import org.junit.Test;
/**
 *
 * Provides a base environment for testing scalar UDF's. Users should extend this class
 * and override the abstract methods. It is highly suggested to test with multiple rows
 * of input because UDFS are stateful in some cases, null, and boundary conditions.
 *
 */
@SuppressWarnings("deprecation")
@Ignore
public abstract class BaseScalarUdfTest {

  /**
   * The data from this method will be fed through the
   * select operator. It is considered the source data
   * for the test.
   * @return The source table that will be fed through the operator tree
   */
  public abstract InspectableObject [] getBaseTable();

  /**
   * The data returned from this UDF will be compared to the results
   * of the test. The DataBuilder class can be used to construct
   * the result.
   * @return The data that will be compared to the results
   */
  public abstract InspectableObject [] getExpectedResult();

  /**
   * Implementors of this method create an expression list. This list
   * transforms the source data into the final output. The DataBuilder
   * class can be used to construct the result.
   * @return A list of expressions
   * @throws UDFArgumentException if the UDF has been formulated incorrectly
   */
  public abstract List<ExprNodeDesc> getExpressionList() throws UDFArgumentException;

  /**
   * This method drives the test. It takes the data from getBaseTable() and
   * feeds it through a SELECT operator with a COLLECT operator after. Each
   * row that is produced by the collect operator is compared to getExpectedResult()
   * and if every row is the expected result the method completes without asserting.
   * @throws HiveException
   */
  @Test
  public final void testUdf() throws HiveException {
    InspectableObject [] data = getBaseTable();
    List<ExprNodeDesc> expressionList = getExpressionList();
    SelectDesc selectCtx = new SelectDesc(expressionList,
        OperatorTestUtils.createOutputColumnNames(expressionList));
    Operator<SelectDesc> op = OperatorFactory.get(new CompilationOpContext(), SelectDesc.class);
    op.setConf(selectCtx);
    CollectDesc cd = new CollectDesc(Integer.valueOf(10));
    CollectOperator cdop = (CollectOperator) OperatorFactory.getAndMakeChild(cd, op);
    op.initialize(new JobConf(OperatorTestUtils.class), new ObjectInspector[] {data[0].oi});
    OperatorTestUtils.assertResults(op, cdop, data, getExpectedResult());
  }

}
