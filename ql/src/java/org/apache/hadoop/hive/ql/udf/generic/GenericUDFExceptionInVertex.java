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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.tez.TezProcessor;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantStringObjectInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the UDF which can throw an exception in arbitrary vertex (typically mapper)
 * / task / task attempt. For throwing exception in reducer side, where most probably
 * GroupByOperator codepath applies, GenericUDAFExceptionInVertex is used.
 */
@Description(name = "exception_in_vertex_udf",
    value = "_FUNC_(hintColumn, vertexName, taskNumberExpression, taskAttemptNumberExpression"
        + "Throws exception in Map tasks, where UDF running is possible",
    extended = "hintColumn is needed for easy-locating mapper stage" +
        " please refer to full examples and explanation in exception_in_vertex_udf.q: "
        + "exception_in_vertex_udf (src1.value, 'Map 1', 0, 0)          -> Map1, first task, first attempt"
        + "exception_in_vertex_udf (src1.value, 'Map 1', '0,1,2', '*')  -> Map1, tasks: 0,1,2, all attempts"
        + "exception_in_vertex_udf (src1.value, 'Map 1', '*', 0)        -> Map1, all tasks, first attempt"
        + "exception_in_vertex_udf (src1.value, 'Map 1', '0-2', '*')    -> Map1, tasks: 0,1,2, all attempts"
        + "exception_in_vertex_udf (src1.value, 'Map 1', '*', '*')      -> Map1, all tasks, all attempts"
        + "exception_in_vertex_udf (src1.value, 'Map 1', '*')           -> Map1, all tasks, all attempts"
        + "exception_in_vertex_udf (src1.value, 'Map 1')                -> Map1, all tasks, all attempts")
public class GenericUDFExceptionInVertex extends GenericUDF {
  private static final Logger LOG = LoggerFactory.getLogger(GenericUDFExceptionInVertex.class);

  private String vertexName;
  private String taskNumberExpr;
  private String taskAttemptNumberExpr;
  private String currentVertexName;
  private int currentTaskNumber;
  private int currentTaskAttemptNumber;
  private boolean alreadyCheckedAndPassed;

  @Override
  public ObjectInspector initialize(ObjectInspector[] parameters) throws UDFArgumentException {
    if (parameters.length < 2) {
      throw new UDFArgumentTypeException(-1,
          "At least two argument is expected (fake column ref, vertex name)");
    }

    this.vertexName = getVertexName(parameters, 1);
    this.taskNumberExpr = getTaskNumber(parameters, 2);
    this.taskAttemptNumberExpr = getTaskAttemptNumber(parameters, 3);

    return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
  }

  public static String getVertexName(ObjectInspector[] parameters, int index) throws UDFArgumentTypeException {
    if (parameters[index] instanceof WritableConstantStringObjectInspector) {
      return ((WritableConstantStringObjectInspector) parameters[index]).getWritableConstantValue()
          .toString();
    } else {
      throw new UDFArgumentTypeException(index, String.format(
          "This argument takes only constant STRING, got %s", parameters[index].getTypeName()));
    }
  }

  public static String getTaskNumber(ObjectInspector[] parameters, int index) throws UDFArgumentTypeException {
    return getExpressionAtIndex(parameters, index);
  }

  public static String getTaskAttemptNumber(ObjectInspector[] parameters, int index) throws UDFArgumentTypeException {
    return getExpressionAtIndex(parameters, index);
  }

  private static String getExpressionAtIndex(ObjectInspector[] parameters, int index) throws UDFArgumentTypeException {
    if (parameters.length > index) {
      if (parameters[index] instanceof WritableConstantStringObjectInspector) {
        return ((WritableConstantStringObjectInspector) parameters[index])
            .getWritableConstantValue().toString();
      } else if (parameters[index] instanceof WritableConstantIntObjectInspector) {
        return ((WritableConstantIntObjectInspector) parameters[index]).getWritableConstantValue()
            .toString();
      } else {
        throw new UDFArgumentTypeException(index, String.format(
            "This argument takes only constant STRING or INT, got %s", parameters[index].getTypeName()));
      }
    } else {
      return "*";
    }
  }

  public static boolean numberFitsExpression(int number, String expression) {
    if (expression.contains("-")) {// "1-10"
      int min = Integer.parseInt(expression.split("-")[0]);
      int max = Integer.parseInt(expression.split("-")[1]);
      return number <= max && number >= min;
    } else if (expression.contains(",")) { // 1,2,3,4
      List<Integer> numbers = Arrays.asList(expression.split(",")).stream()
          .map(x -> Integer.valueOf(x)).collect(Collectors.toList());
      return numbers.contains(number);
    } else if ("*".equals(expression)) {
      return true;
    } else {
      return Integer.parseInt(expression) == number;
    }
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
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (alreadyCheckedAndPassed) {
      return 0L;
    }

    LOG.debug("evaluate: vertex {}, task: {}, attempt: {} <-> vertex {}, task: {}, attempt: {}",
        currentVertexName, currentTaskNumber, currentTaskAttemptNumber, vertexName, taskNumberExpr,
        taskAttemptNumberExpr);

    if (vertexName.equals(currentVertexName)
        && GenericUDFExceptionInVertex.numberFitsExpression(currentTaskNumber, taskNumberExpr)
        && GenericUDFExceptionInVertex.numberFitsExpression(currentTaskAttemptNumber,
            taskAttemptNumberExpr)) {
      String message = String.format(
          "GenericUDFExceptionInVertex: found condition for throwing exception (vertex/task/attempt):"
              + "current %s / %d / %d matches criteria %s / %s / %s",
          currentVertexName, currentTaskNumber, currentTaskAttemptNumber, vertexName,
          taskNumberExpr, taskAttemptNumberExpr);
      LOG.info(message);
      throw new HiveException(message);
    } else {
      alreadyCheckedAndPassed = true;
      return 0L;
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("GenericUDFExceptionInVertex", children, ",");
  }
}
