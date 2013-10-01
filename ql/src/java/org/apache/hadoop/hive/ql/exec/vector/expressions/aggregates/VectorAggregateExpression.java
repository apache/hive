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

package org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates;

import java.io.Serializable;

import org.apache.hadoop.hive.ql.exec.vector.VectorAggregationBufferRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * Base class for aggregation expressions.
 */
public abstract class VectorAggregateExpression  implements Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * Buffer interface to store aggregates.
   */
  public static interface AggregationBuffer extends Serializable {
    int getVariableSize();
  };

  public abstract AggregationBuffer getNewAggregationBuffer() throws HiveException;
  public abstract void aggregateInput(AggregationBuffer agg, VectorizedRowBatch unit)
        throws HiveException;
  public abstract void aggregateInputSelection(VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregateIndex, VectorizedRowBatch vrg) throws HiveException;
  public abstract void reset(AggregationBuffer agg) throws HiveException;
  public abstract Object evaluateOutput(AggregationBuffer agg) throws HiveException;

  public abstract ObjectInspector getOutputObjectInspector();
  public abstract int getAggregationBufferFixedSize();
  public boolean hasVariableSize() {
    return false;
  }

  public abstract void init(AggregationDesc desc) throws HiveException;
}

