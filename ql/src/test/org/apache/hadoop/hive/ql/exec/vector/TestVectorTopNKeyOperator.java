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

import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.physical.Vectorizer;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.TopNKeyDesc;
import org.apache.hadoop.hive.ql.plan.VectorTopNKeyDesc;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;
import java.util.List;

public class TestVectorTopNKeyOperator extends TestVectorOperator {

  @Test
  public void testTopNHasSelectedSmallerThanBatchDoesNotThrowException() throws HiveException {
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

    FakeDataReader fdr = new FakeDataReader(1024, 3, FakeDataSampleType.Repeated);
    VectorizedRowBatch vrg = fdr.getNext();

    vrg.selected = new int[] { 1, 2, 3, 4};

    Assertions.assertDoesNotThrow(() -> vfo.process(vrg, 0));
  }
}
