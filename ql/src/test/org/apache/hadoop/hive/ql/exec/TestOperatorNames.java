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
 
package org.apache.hadoop.hive.ql.exec;



import org.apache.hadoop.hive.ql.exec.vector.VectorAppMasterEventOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorFilterOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorGroupByOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorLimitOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorMapJoinOuterFilteredOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorMapOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorSMBMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorSelectOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorSparkHashTableSinkOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorSparkPartitionPruningSinkOperator;
import org.apache.hadoop.hive.ql.parse.spark.SparkPartitionPruningSinkOperator;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

/**
 * OperatorNames Test.
 */
public class TestOperatorNames {

  @Before
  public void setUp() throws Exception {

  }

  @After
  public void tearDown() throws Exception {

  }

  /*
   * If there's a mismatch between static and object name, or a mismatch between
   * vector and non-vector operator name, the optimizer doens't work correctly.
   */
  @Test
  public void testOperatorNames() throws Exception {

    assertEquals(SelectOperator.getOperatorName(), new SelectOperator().getName());
    assertEquals(SelectOperator.getOperatorName(), new VectorSelectOperator().getName());

    assertEquals(GroupByOperator.getOperatorName(), new GroupByOperator().getName());
    assertEquals(GroupByOperator.getOperatorName(), new VectorGroupByOperator().getName());

    assertEquals(FilterOperator.getOperatorName(), new FilterOperator().getName());
    assertEquals(FilterOperator.getOperatorName(), new VectorFilterOperator().getName());

    assertEquals(LimitOperator.getOperatorName(), new LimitOperator().getName());
    assertEquals(LimitOperator.getOperatorName(), new VectorLimitOperator().getName());

    assertEquals(MapOperator.getOperatorName(), new MapOperator().getName());
    assertEquals(MapOperator.getOperatorName(), new VectorMapOperator().getName());

    assertEquals(MapJoinOperator.getOperatorName(), new MapJoinOperator().getName());
    assertEquals(MapJoinOperator.getOperatorName(), new VectorMapJoinOperator().getName());

    assertEquals(AppMasterEventOperator.getOperatorName(), new AppMasterEventOperator().getName());
    assertEquals(AppMasterEventOperator.getOperatorName(),
        new VectorAppMasterEventOperator().getName());

    assertEquals(SMBMapJoinOperator.getOperatorName(), new SMBMapJoinOperator().getName());
    assertEquals(SMBMapJoinOperator.getOperatorName(), new VectorSMBMapJoinOperator().getName());

    assertEquals(MapJoinOperator.getOperatorName(),
        new VectorMapJoinOuterFilteredOperator().getName());

    assertEquals(SparkHashTableSinkOperator.getOperatorName(),
        new SparkHashTableSinkOperator().getName());
    assertEquals(SparkHashTableSinkOperator.getOperatorName(),
        new VectorSparkHashTableSinkOperator().getName());

    assertEquals(SparkPartitionPruningSinkOperator.getOperatorName(),
        new SparkPartitionPruningSinkOperator().getName());
    assertEquals(SparkPartitionPruningSinkOperator.getOperatorName(),
        new VectorSparkPartitionPruningSinkOperator().getName());

  }

}
