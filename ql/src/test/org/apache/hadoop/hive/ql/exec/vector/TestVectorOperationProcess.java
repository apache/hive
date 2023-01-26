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

package org.apache.hadoop.hive.ql.exec.vector;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FilterExprAndExpr;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColEqualDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColGreaterLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.util.FakeDataReader;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.physical.Vectorizer;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.TopNKeyDesc;
import org.apache.hadoop.hive.ql.plan.VectorFilterDesc;
import org.apache.hadoop.hive.ql.plan.VectorTopNKeyDesc;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests that are necessary until VectorizedRowBatch selected issue is being fixed:
 * It has three fields to manage selected elements:
 * - selected (array)
 * - selectedInUse
 * - size (selected size)
 * <p>
 * The tricky thing is those fields are all public: so it is possible to modify some of them without keeping the others
 * in sync.
 * The issue that is fixed and checked with these tests is when somebody modifies the selected array but forgets to
 * set the size parameter: Because of that, the Operators can try to process with wrong size value and it can cause
 * ArrayIndexOutOfBoundsException.
 * </p>
 * <p>
 * Related ticket: HIVE-26992
 * Those tests can be removed when those fields are public they can be modified with public methods that ensures to
 * use them properly.
 * </p>
 * Ticket to fix the issue: HIVE-26993
 */
public class TestVectorOperationProcess {

  HiveConf hiveConf = new HiveConf();

  @Test
  public void testVectorFilterHasSelectedSmallerThanBatch() throws HiveException {
    ExprNodeColumnDesc col1Expr = new ExprNodeColumnDesc(Long.class, "col1", "table", false);
    List<String> columns = new ArrayList<>();
    columns.add("col1");
    FilterDesc filterDesc = new FilterDesc();
    filterDesc.setPredicate(col1Expr);
    VectorFilterDesc vectorDesc = new VectorFilterDesc();

    Operator<? extends OperatorDesc> filterOp =
            OperatorFactory.get(new CompilationOpContext(), filterDesc);

    VectorizationContext vc = new VectorizationContext("name", columns);

    VectorFilterOperator vfo = (VectorFilterOperator) Vectorizer.vectorizeFilterOperator(filterOp, vc, vectorDesc);

    vfo.initialize(hiveConf, null);
    VectorExpression ve1 = new FilterLongColGreaterLongColumn(0,1);
    VectorExpression ve2 = new FilterLongColEqualDoubleScalar(2, 0);
    VectorExpression ve3 = new FilterExprAndExpr();
    ve3.setChildExpressions(new VectorExpression[] {ve1, ve2});
    vfo.setFilterCondition(ve3);

    FakeDataReader fdr = new FakeDataReader(1024, 3);
    VectorizedRowBatch vrg = fdr.getNext();

    vrg.selected = new int[] { 1, 2, 3, 4};

    vfo.process(vrg, 0);
  }

  @Test
  public void testTopNHasSelectedSmallerThanBatch() throws HiveException {
    List<String> columns = new ArrayList<>();
    columns.add("col1");
    TopNKeyDesc topNKeyDesc = new TopNKeyDesc();
    topNKeyDesc.setCheckEfficiencyNumBatches(1);
    topNKeyDesc.setTopN(2);

    Operator<? extends OperatorDesc> filterOp =
            OperatorFactory.get(new CompilationOpContext(), topNKeyDesc);

    VectorizationContext vc = new VectorizationContext("name", columns);

    VectorTopNKeyOperator vfo = (VectorTopNKeyOperator) Vectorizer.vectorizeTopNKeyOperator(filterOp, vc, new VectorTopNKeyDesc());

    vfo.initialize(hiveConf, null);
    VectorExpression ve1 = new FilterLongColGreaterLongColumn(0,1);
    VectorExpression ve2 = new FilterLongColEqualDoubleScalar(2, 0);
    VectorExpression ve3 = new FilterExprAndExpr();
    ve3.setChildExpressions(new VectorExpression[] {ve1, ve2});

    FakeDataReader fdr = new FakeDataReader(1024, 3);
    VectorizedRowBatch vrg = fdr.getNext();

    vrg.selected = new int[] { 1, 2, 3, 4};

    vfo.process(vrg, 0);
  }
}
