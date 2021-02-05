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
package org.apache.hadoop.hive.ql.udf.generic;

import java.io.Serializable;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.tez.TezProcessor;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the UDAF which can throw an exception in arbitrary aggregating vertex
 * (typically reducer) / task / task attempt.
 */
@Description(name = "exception_in_vertex_udaf",
    value = "_FUNC_(vertexName, taskNumberExpression, taskAttemptNumberExpression)"
        + "Throws exception in Reducer tasks, where UDF running is possible",
    extended = " please refer to full examples in exception_in_vertex_udf.q: "
        + "exception_in_vertex_udaf ('Reducer 2', 0, 0)          -> Reducer2, first task, first attempt"
        + "exception_in_vertex_udaf ('Reducer 2', '0,1,2', '*')  -> Reducer2, tasks: 0,1,2, all attempts"
        + "exception_in_vertex_udaf ('Reducer 2', '*', 0)        -> Reducer2, all tasks, first attempt"
        + "exception_in_vertex_udaf ('Reducer 2', '0-2', '*')    -> Reducer2, tasks: 0,1,2, all attempts"
        + "exception_in_vertex_udaf ('Reducer 2', '*', '*')      -> Reducer2, all tasks, all attempts"
        + "exception_in_vertex_udaf ('Reducer 2', '*')           -> Reducer2, all tasks, all attempts"
        + "exception_in_vertex_udaf ('Reducer 2')                -> Reducer2, all tasks, all attempts")
public class GenericUDAFExceptionInVertex implements GenericUDAFResolver2 {
  private static final Logger LOG = LoggerFactory.getLogger(GenericUDAFExceptionInVertex.class);

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
    return new GenericUDAFExceptionInVertexEvaluator();
  }

  @Override
  public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo paramInfo)
      throws SemanticException {
    ObjectInspector[] parameters = paramInfo.getParameterObjectInspectors();

    if (parameters.length < 1) {
      throw new UDFArgumentTypeException(-1, "At least one argument is expected (vertex name)");
    }

    String vertexName = GenericUDFExceptionInVertex.getVertexName(parameters, 0);
    String taskNumber = GenericUDFExceptionInVertex.getTaskNumber(parameters, 1);
    String taskAttemptNumber = GenericUDFExceptionInVertex.getTaskAttemptNumber(parameters, 2);

    GenericUDAFExceptionInVertexEvaluator evaluator =
        new GenericUDAFExceptionInVertexEvaluator(vertexName, taskNumber, taskAttemptNumber);
    return evaluator;
  }

  /**
   * GenericUDAFExceptionInVertexEvaluator.
   *
   */
  public static class GenericUDAFExceptionInVertexEvaluator extends GenericUDAFEvaluator
      implements Serializable {
    private String vertexName;
    private String taskNumberExpr;
    private String taskAttemptNumberExpr;

    private transient String currentVertexName;
    private transient int currentTaskNumber;
    private transient int currentTaskAttemptNumber;

    private boolean alreadyCheckedAndPassed = false;

    public GenericUDAFExceptionInVertexEvaluator() {
    }

    public GenericUDAFExceptionInVertexEvaluator(String vertexName, String taskNumber,
        String taskAttemptNumber) {
      this.vertexName = vertexName;
      this.taskNumberExpr = taskNumber;
      this.taskAttemptNumberExpr = taskAttemptNumber;
    }

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);
      return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    }

    @Override
    public void configure(MapredContext mapredContext) {
      this.currentVertexName = mapredContext.getJobConf().get(TezProcessor.HIVE_TEZ_VERTEX_NAME);
      this.currentTaskNumber =
          mapredContext.getJobConf().getInt(TezProcessor.HIVE_TEZ_TASK_INDEX, -1);
      this.currentTaskAttemptNumber =
          mapredContext.getJobConf().getInt(TezProcessor.HIVE_TEZ_TASK_ATTEMPT_NUMBER, -1);

      LOG.debug(
          "configure vertex: {}, task: {}, attempt: {} <-> current vertex {}, task: {}, attempt: {}",
          vertexName, taskNumberExpr, taskAttemptNumberExpr, currentVertexName, currentTaskNumber,
          currentTaskAttemptNumber);
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      return new AggregationBuffer() {
      };
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      checkAndThrowException("iterate");
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      checkAndThrowException("merge");
    }

    private void checkAndThrowException(String phase) throws HiveException {
      if (alreadyCheckedAndPassed) {
        return;
      }
      LOG.debug("{}: vertex {}, task: {}, attempt: {} <-> vertex {}, task: {}, attempt: {}", phase,
          currentVertexName, currentTaskNumber, currentTaskAttemptNumber, vertexName,
          taskNumberExpr, taskAttemptNumberExpr);

      if (vertexName.equals(currentVertexName)
          && GenericUDFExceptionInVertex.numberFitsExpression(currentTaskNumber, taskNumberExpr)
          && GenericUDFExceptionInVertex.numberFitsExpression(currentTaskAttemptNumber,
              taskAttemptNumberExpr)) {
        String message = String.format(
            "GenericUDAFExceptionInVertex: found condition for throwing exception (vertex/task/attempt):"
                + "current %s / %d / %d matches criteria %s / %s / %s",
            currentVertexName, currentTaskNumber, currentTaskAttemptNumber, vertexName,
            taskNumberExpr, taskAttemptNumberExpr);
        LOG.info(message);
        throw new HiveException(message);
      } else {
        alreadyCheckedAndPassed = true;
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      return null;
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      return null;
    }
  }
}
