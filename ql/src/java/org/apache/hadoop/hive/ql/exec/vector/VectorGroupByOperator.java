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

package org.apache.hadoop.hive.ql.exec.vector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorAggregateExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates
    .VectorAggregateExpression.AggregationBuffer;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;

/**
 * Vectorized GROUP BY operator impelementation. Consumes the vectorized input and
 * stores the aggregates operators intermediate states. Emits row mode output.
 *
 */
public class VectorGroupByOperator extends Operator<GroupByDesc> implements Serializable {

  private final VectorizationContext vContext;

  protected transient VectorAggregateExpression[] aggregators;
  protected transient AggregationBuffer[] aggregationBuffers;

  transient int heartbeatInterval;
  transient int countAfterReport;

  private static final long serialVersionUID = 1L;

  public VectorGroupByOperator(VectorizationContext ctxt, OperatorDesc conf) {
    super();
    this.vContext = ctxt;
    this.conf = (GroupByDesc) conf;
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {

    List<ObjectInspector> objectInspectors = new ArrayList<ObjectInspector>();

    try {
      vContext.setOperatorType(OperatorType.GROUPBY);

      ArrayList<AggregationDesc> aggrDesc = conf.getAggregators();

      aggregators = new VectorAggregateExpression[aggrDesc.size()];
      aggregationBuffers = new AggregationBuffer[aggrDesc.size()];
      for (int i = 0; i < aggrDesc.size(); ++i) {
        AggregationDesc desc = aggrDesc.get(i);
        aggregators[i] = vContext.getAggregatorExpression (desc);
        aggregationBuffers[i] = aggregators[i].getNewAggregationBuffer();
        aggregators[i].reset(aggregationBuffers[i]);

        objectInspectors.add(aggregators[i].getOutputObjectInspector());
      }

      List<String> outputFieldNames = conf.getOutputColumnNames();
      outputObjInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
          outputFieldNames, objectInspectors);

    } catch (HiveException he) {
      throw he;
    } catch (Throwable e) {
      throw new HiveException(e);
    }
    initializeChildren(hconf);
  }

  @Override
  public void processOp(Object row, int tag) throws HiveException {
    VectorizedRowBatch vrg = (VectorizedRowBatch) row;

    //TODO: proper group by hash
    for (int i = 0; i < aggregators.length; ++i) {
      aggregators[i].aggregateInput(aggregationBuffers[i], vrg);
    }
  }

  @Override
  public void closeOp(boolean aborted) throws HiveException {
    if (!aborted) {
      Object[] forwardCache = new Object[aggregators.length];
      for (int i = 0; i < aggregators.length; ++i) {
        forwardCache[i] = aggregators[i].evaluateOutput(aggregationBuffers[i]);
      }
      forward(forwardCache, outputObjInspector);
    }
  }

  /**
   * @return the name of the operator
   */
  @Override
  public String getName() {
    return getOperatorName();
  }

  static public String getOperatorName() {
    return "GBY";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.GROUPBY;
  }

}

