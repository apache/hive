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

package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.PTFPartition;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ptf.PTFExpressionDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.ql.udf.ptf.BasePartitionEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class TestGenericUDAFEvaluator {

  @Mock(answer = Answers.CALLS_REAL_METHODS)
  private GenericUDAFEvaluator udafEvaluator;

  @Mock
  private WindowFrameDef winFrame;

  @Mock
  private PTFPartition partition1;

  @Mock
  private ObjectInspector outputOI;

  private List<PTFExpressionDef> parameters = Collections.emptyList();

  @Test
  public void testGetPartitionWindowingEvaluatorWithoutInitCall() {
    BasePartitionEvaluator partition1Evaluator1 = udafEvaluator.getPartitionWindowingEvaluator(
        winFrame, partition1, parameters, outputOI);

    BasePartitionEvaluator partition1Evaluator2 = udafEvaluator.getPartitionWindowingEvaluator(
        winFrame, partition1, parameters, outputOI);

    Assert.assertEquals(partition1Evaluator1, partition1Evaluator2);
  }

  @Test
  public void testGetPartitionWindowingEvaluatorWithInitCall() throws HiveException {
    BasePartitionEvaluator partition1Evaluator1 = udafEvaluator.getPartitionWindowingEvaluator(
        winFrame, partition1, parameters, outputOI);

    udafEvaluator.init(GenericUDAFEvaluator.Mode.COMPLETE, null);

    BasePartitionEvaluator newPartitionEvaluator = udafEvaluator.getPartitionWindowingEvaluator(
        winFrame, partition1, parameters, outputOI);

    Assert.assertNotEquals(partition1Evaluator1, newPartitionEvaluator);
  }

}
